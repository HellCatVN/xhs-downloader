from asyncio import (
    Event,
    Queue,
    QueueEmpty,
    create_task,
    gather,
    sleep,
    Future,
    CancelledError,
)
from contextlib import suppress
from datetime import datetime
from re import compile
from urllib.parse import urlencode, urlparse
from textwrap import dedent
from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastmcp import FastMCP
from typing import Annotated
from pydantic import Field
from types import SimpleNamespace
from pyperclip import copy, paste
from uvicorn import Config, Server
from typing import Callable

from ..expansion import (
    # BrowserCookie,
    Cleaner,
    Converter,
    Namespace,
    beautify_string,
)
from ..module import (
    __VERSION__,
    ERROR,
    MASTER,
    REPOSITORY,
    ROOT,
    VERSION_BETA,
    VERSION_MAJOR,
    VERSION_MINOR,
    WARNING,
    CreatorBatchData,
    CreatorBatchParams,
    DataRecorder,
    ExtractData,
    ExtractParams,
    IDRecorder,
    Manager,
    MapRecorder,
    NoteSummary,
    logging,
    # sleep_time,
    ScriptServer,
    INFO,
)
from ..translation import _, switch_language

from ..module import Mapping
from .download import Download
from .explore import Explore
from .image import Image
from .request import Html
from .user_posted import UserPosted
from .video import Video
from rich import print

__all__ = ["XHS"]


def data_cache(function):
    async def inner(
        self,
        data: dict,
    ):
        if self.manager.record_data:
            download = data["下载地址"]
            lives = data["动图地址"]
            await function(
                self,
                data,
            )
            data["下载地址"] = download
            data["动图地址"] = lives

    return inner


class Print:
    def __init__(
        self,
        func: Callable = print,
    ):
        self.func = func

    def __call__(
        self,
    ):
        return self.func


class XHS:
    VERSION_MAJOR = VERSION_MAJOR
    VERSION_MINOR = VERSION_MINOR
    VERSION_BETA = VERSION_BETA
    LINK_XHS = compile(r"(?:https?://)?www\.xiaohongshu\.com/explore/\S+")
    LINK_RN = compile(r"(?:https?://)?www\.rednote\.com/explore/\S+")
    USER_XHS = compile(
        r"(?:https?://)?www\.xiaohongshu\.com/user/profile/[a-z0-9]+/\S+"
    )
    USER_RN = compile(r"(?:https?://)?www\.rednote\.com/user/profile/[a-z0-9]+/\S+")
    SHARE_XHS = compile(r"(?:https?://)?www\.xiaohongshu\.com/discovery/item/\S+")
    SHARE_RN = compile(r"(?:https?://)?www\.rednote\.com/discovery/item/\S+")
    SHORT = compile(r"(?:https?://)?xhslink\.com/[^\s\"<>\\^`{|}，。；！？、【】《》]+")
    ID = compile(r"(?:explore|item)/(\S+)?\?")
    ID_USER = compile(r"user/profile/[a-z0-9]+/(\S+)?\?")
    CREATOR_PROFILE = compile(
        r"^(?:https?://)?(?:www\.)?(?:xiaohongshu\.com|rednote\.com)/user/profile/([^/?#]+)(?:[/?#].*)?$"
    )
    CREATOR_ID = compile(r"^[a-zA-Z0-9_-]+$")
    __INSTANCE = None
    CLEANER = Cleaner()

    def __new__(cls, *args, **kwargs):
        if not cls.__INSTANCE:
            cls.__INSTANCE = super().__new__(cls)
        return cls.__INSTANCE

    def __init__(
        self,
        mapping_data: dict = None,
        work_path="",
        folder_name="Download",
        name_format="发布时间 作者昵称 作品标题",
        user_agent: str = None,
        cookie: str = "",
        proxy: str | dict = None,
        timeout=10,
        chunk=1024 * 1024,
        max_retry=5,
        record_data=False,
        image_format="JPEG",
        image_download=True,
        video_download=True,
        live_download=False,
        video_preference="resolution",
        folder_mode=False,
        download_record=True,
        author_archive=False,
        write_mtime=False,
        language="zh_CN",
        # read_cookie: int | str = None,
        script_server: bool = False,
        script_host="0.0.0.0",
        script_port=5558,
        **kwargs,
    ):
        switch_language(language)
        self.print = Print()
        self.manager = Manager(
            ROOT,
            work_path,
            folder_name,
            name_format,
            chunk,
            user_agent,
            cookie,
            # self.read_browser_cookie(read_cookie) or cookie,
            proxy,
            timeout,
            max_retry,
            record_data,
            image_format,
            image_download,
            video_download,
            live_download,
            video_preference,
            download_record,
            folder_mode,
            author_archive,
            write_mtime,
            script_server,
            self.CLEANER,
            self.print,
        )
        self.mapping_data = mapping_data or {}
        self.map_recorder = MapRecorder(
            self.manager,
        )
        self.mapping = Mapping(self.manager, self.map_recorder)
        self.html = Html(self.manager)
        self.image = Image()
        self.video = Video()
        self.explore = Explore()
        self.convert = Converter()
        self.download = Download(self.manager)
        self.id_recorder = IDRecorder(self.manager)
        self.data_recorder = DataRecorder(self.manager)
        self.clipboard_cache: str = ""
        self.queue = Queue()
        self.event = Event()
        self.script = None
        self.init_script_server(
            script_host,
            script_port,
        )

    def __extract_image(self, container: dict, data: Namespace):
        container["下载地址"], container["动图地址"] = self.image.get_image_link(
            data, self.manager.image_format
        )

    def __extract_video(
        self,
        container: dict,
        data: Namespace,
    ):
        container["下载地址"] = self.video.deal_video_link(
            data,
            self.manager.video_preference,
        )
        container["动图地址"] = [
            None,
        ]

    async def __download_files(
        self,
        container: dict,
        download: bool,
        index,
        count: SimpleNamespace,
    ):
        name = self.__naming_rules(container)
        if (u := container["下载地址"]) and download:
            if await self.skip_download(i := container["作品ID"]):
                self.logging(_("作品 {0} 存在下载记录，跳过下载").format(i))
                count.skip += 1
            else:
                __, result = await self.download.run(
                    u,
                    container["动图地址"],
                    index,
                    container["作者ID"]
                    + "_"
                    + self.CLEANER.filter_name(container["作者昵称"]),
                    name,
                    container["作品类型"],
                    container["时间戳"],
                )
                if not result:
                    count.skip += 1
                elif all(result):
                    count.success += 1
                    await self.__add_record(
                        i,
                    )
                else:
                    count.fail += 1
        elif not u:
            self.logging(_("提取作品文件下载地址失败"), ERROR)
            count.fail += 1
        await self.save_data(container)

    @data_cache
    async def save_data(
        self,
        data: dict,
    ):
        data["采集时间"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        data["下载地址"] = " ".join(data["下载地址"])
        data["动图地址"] = " ".join(i or "NaN" for i in data["动图地址"])
        data.pop("时间戳", None)
        await self.data_recorder.add(**data)

    async def __add_record(
        self,
        id_: str,
    ) -> None:
        await self.id_recorder.add(id_)

    async def extract(
        self,
        url: str,
        download=False,
        index: list | tuple = None,
        data=True,
    ) -> list[dict]:
        if not (
            urls := await self.extract_links(
                url,
            )
        ):
            self.logging(_("提取小红书作品链接失败"), WARNING)
            return []
        statistics = SimpleNamespace(
            all=len(urls),
            success=0,
            fail=0,
            skip=0,
        )
        self.logging(_("共 {0} 个小红书作品待处理...").format(statistics.all))
        result = [
            await self.__deal_extract(
                i,
                download,
                index,
                data,
                count=statistics,
            )
            for i in urls
        ]
        self.show_statistics(
            statistics,
        )
        return result

    def show_statistics(
        self,
        statistics: SimpleNamespace,
    ) -> None:
        self.logging(
            _("共处理 {0} 个作品，成功 {1} 个，失败 {2} 个，跳过 {3} 个").format(
                statistics.all,
                statistics.success,
                statistics.fail,
                statistics.skip,
            ),
        )

    async def extract_cli(
        self,
        url: str,
        download=True,
        index: list | tuple = None,
        data=False,
    ) -> None:
        url = await self.extract_links(
            url,
        )
        if not url:
            self.logging(_("提取小红书作品链接失败"), WARNING)
            return
        if index:
            await self.__deal_extract(
                url[0],
                download,
                index,
                data,
            )
        else:
            statistics = SimpleNamespace(
                all=len(url),
                success=0,
                fail=0,
                skip=0,
            )
            [
                await self.__deal_extract(
                    u,
                    download,
                    index,
                    data,
                    count=statistics,
                )
                for u in url
            ]
            self.show_statistics(
                statistics,
            )

    async def extract_links(
        self,
        url: str,
    ) -> list:
        urls = []
        for i in url.split():
            if u := self.SHORT.search(i):
                i = await self.html.request_url(
                    u.group(),
                    False,
                )
            if u := self.SHARE_XHS.search(i):
                urls.append(u.group())
            elif u := self.SHARE_RN.search(i):
                urls.append(u.group())
            elif u := self.LINK_XHS.search(i):
                urls.append(u.group())
            elif u := self.LINK_RN.search(i):
                urls.append(u.group())
            elif u := self.USER_XHS.search(i):
                urls.append(u.group())
            elif u := self.USER_RN.search(i):
                urls.append(u.group())
        return urls

    def extract_id(self, links: list[str]) -> list[str]:
        ids = []
        for i in links:
            if j := self.ID.search(i):
                ids.append(j.group(1))
            elif j := self.ID_USER.search(i):
                ids.append(j.group(1))
        return ids

    async def _get_html_data(
        self,
        url: str,
        data: bool,
        cookie: str = None,
        proxy: str = None,
        count=SimpleNamespace(
            all=0,
            success=0,
            fail=0,
            skip=0,
        ),
    ) -> tuple[str, Namespace | dict]:
        if await self.skip_download(id_ := self.__extract_link_id(url)) and not data:
            msg = _("作品 {0} 存在下载记录，跳过处理").format(id_)
            self.logging(msg)
            count.skip += 1
            return id_, {"message": msg}
        self.logging(_("开始处理作品：{0}").format(id_))
        html = await self.html.request_url(
            url,
            cookie=cookie,
            proxy=proxy,
        )
        namespace = self.__generate_data_object(html)
        if not namespace:
            self.logging(_("{0} 获取数据失败").format(id_), ERROR)
            count.fail += 1
            return id_, {}
        return id_, namespace

    def _extract_data(
        self,
        namespace: Namespace,
        id_: str,
        count,
    ):
        data = self.explore.run(namespace)
        if not data:
            self.logging(_("{0} 提取数据失败").format(id_), ERROR)
            count.fail += 1
            return {}
        return data

    async def _deal_download_tasks(
        self,
        data: dict,
        namespace: Namespace,
        id_: str,
        download: bool,
        index: list | tuple | None,
        count: SimpleNamespace,
    ):
        if data["作品类型"] == _("视频"):
            self.__extract_video(data, namespace)
        elif data["作品类型"] in {
            _("图文"),
            _("图集"),
        }:
            self.__extract_image(data, namespace)
        else:
            self.logging(_("未知的作品类型：{0}").format(id_), WARNING)
            data["下载地址"] = []
            data["动图地址"] = []
        await self.update_author_nickname(
            data,
        )
        await self.__download_files(
            data,
            download,
            index,
            count,
        )
        # await sleep_time()
        return data

    async def __deal_extract(
        self,
        url: str,
        download: bool,
        index: list | tuple | None,
        data: bool,
        cookie: str = None,
        proxy: str = None,
        count=SimpleNamespace(
            all=0,
            success=0,
            fail=0,
            skip=0,
        ),
    ):
        id_, namespace = await self._get_html_data(
            url,
            data,
            cookie,
            proxy,
            count,
        )
        if not isinstance(namespace, Namespace):
            return namespace
        if not (
            data := self._extract_data(
                namespace,
                id_,
                count,
            )
        ):
            return data
        data = await self._deal_download_tasks(
            data
            | {
                "作品链接": url,
            },
            namespace,
            id_,
            download,
            index,
            count,
        )
        self.logging(_("作品处理完成：{0}").format(id_))
        return data

    async def deal_script_tasks(
        self,
        data: dict,
        index: list | tuple | None,
        count=SimpleNamespace(
            all=0,
            success=0,
            fail=0,
            skip=0,
        ),
    ):
        namespace = self.json_to_namespace(data)
        id_ = namespace.safe_extract("noteId", "")
        if not (
            data := self._extract_data(
                namespace,
                id_,
                count,
            )
        ):
            return data
        return await self._deal_download_tasks(
            data,
            namespace,
            id_,
            True,
            index,
            count,
        )

    @staticmethod
    def json_to_namespace(data: dict) -> Namespace:
        return Namespace(data)

    async def update_author_nickname(
        self,
        container: dict,
    ):
        if a := self.CLEANER.filter_name(
            self.mapping_data.get(i := container["作者ID"], "")
        ):
            container["作者昵称"] = a
        else:
            container["作者昵称"] = self.manager.filter_name(container["作者昵称"]) or i
        await self.mapping.update_cache(
            i,
            container["作者昵称"],
        )

    @staticmethod
    def __extract_link_id(url: str) -> str:
        link = urlparse(url)
        return link.path.split("/")[-1]

    def __generate_data_object(self, html: str) -> Namespace:
        data = self.convert.run(html)
        return Namespace(data)

    def __naming_rules(self, data: dict) -> str:
        keys = self.manager.name_format.split()
        values = []
        for key in keys:
            match key:
                case "发布时间":
                    values.append(self.__get_name_time(data))
                case "作品标题":
                    values.append(self.__get_name_title(data))
                case _:
                    values.append(data[key])
        return beautify_string(
            self.CLEANER.filter_name(
                self.manager.SEPARATE.join(values),
                default=self.manager.SEPARATE.join(
                    (
                        data["作者ID"],
                        data["作品ID"],
                    )
                ),
            ),
            length=128,
        )

    @staticmethod
    def __get_name_time(data: dict) -> str:
        return data["发布时间"].replace(":", ".")

    def __get_name_title(self, data: dict) -> str:
        return (
            beautify_string(
                self.manager.filter_name(data["作品标题"]),
                64,
            )
            or data["作品ID"]
        )

    async def monitor(
        self,
        delay=1,
        download=True,
        data=False,
    ) -> None:
        self.logging(
            _(
                "程序会自动读取并提取剪贴板中的小红书作品链接，并自动下载链接对应的作品文件，如需关闭，请点击关闭按钮，或者向剪贴板写入 “close” 文本！"
            ),
            style=MASTER,
        )
        self.event.clear()
        copy("")
        await gather(
            self.__get_link(delay),
            self.__receive_link(delay, download=download, index=None, data=data),
        )

    async def __get_link(self, delay: int):
        while not self.event.is_set():
            if (t := paste()).lower() == "close":
                self.stop_monitor()
            elif t != self.clipboard_cache:
                self.clipboard_cache = t
                create_task(self.__push_link(t))
            await sleep(delay)

    async def __push_link(
        self,
        content: str,
    ):
        await gather(
            *[
                self.queue.put(i)
                for i in await self.extract_links(
                    content,
                )
            ]
        )

    async def __receive_link(self, delay: int, *args, **kwargs):
        while not self.event.is_set() or self.queue.qsize() > 0:
            with suppress(QueueEmpty):
                await self.__deal_extract(self.queue.get_nowait(), *args, **kwargs)
            await sleep(delay)

    def stop_monitor(self):
        self.event.set()

    async def skip_download(self, id_: str) -> bool:
        return bool(await self.id_recorder.select(id_))

    async def __aenter__(self):
        await self.id_recorder.__aenter__()
        await self.data_recorder.__aenter__()
        await self.map_recorder.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.id_recorder.__aexit__(exc_type, exc_value, traceback)
        await self.data_recorder.__aexit__(exc_type, exc_value, traceback)
        await self.map_recorder.__aexit__(exc_type, exc_value, traceback)
        await self.close()

    async def close(self):
        await self.stop_script_server()
        await self.manager.close()

    def _creator_identity(self, value: str) -> tuple[str, str] | None:
        value = value.strip()
        if match := self.CREATOR_PROFILE.fullmatch(value):
            profile_url = value if value.startswith("http") else f"https://{value}"
            return match.group(1), profile_url
        user_id = value.removeprefix("@")
        if self.CREATOR_ID.fullmatch(user_id):
            return user_id, f"https://www.xiaohongshu.com/user/profile/{user_id}"
        return None

    @staticmethod
    def _creator_note_url(note: dict) -> str:
        note_id = note["note_id"]
        if not (token := note.get("xsec_token")):
            return f"https://www.xiaohongshu.com/explore/{note_id}"
        query = urlencode(
            {
                "source": "webshare",
                "xhsshare": "pc_web",
                "xsec_token": token,
                "xsec_source": "pc_share",
            }
        )
        return f"https://www.xiaohongshu.com/discovery/item/{note_id}?{query}"

    @staticmethod
    def _media_urls(value) -> list[str]:
        if isinstance(value, str):
            value = value.split()
        if not isinstance(value, list):
            return []
        return [str(item) for item in value if item and str(item).lower() != "nan"]

    def _note_summary(self, note: dict, data: dict) -> NoteSummary | None:
        note_id = data.get("作品ID") or note["note_id"]
        images = self._media_urls(data.get("下载地址"))
        live = self._media_urls(data.get("动图地址"))
        work_type = data.get("作品类型")
        cover_url = note.get("cover_url")
        if live:
            kind = "livephoto"
            media_urls = list(dict.fromkeys([*images, *live]))
            cover_url = cover_url or (images[0] if images else None)
        elif work_type == _("视频"):
            kind = "video"
            media_urls = images
            if not cover_url:
                return None
        else:
            kind = "images"
            media_urls = images
            cover_url = cover_url or (images[0] if images else None)
        if not media_urls:
            return None
        return NoteSummary(
            note_id=str(note_id),
            desc=str(data.get("作品描述") or data.get("作品标题") or ""),
            kind=kind,
            media_urls=media_urls,
            cover_url=cover_url,
        )

    async def _resolve_creator_notes(
        self,
        notes: list[dict],
        extract: CreatorBatchParams,
        attempted: set[str],
    ) -> tuple[list[NoteSummary], int]:
        result = []
        errors = 0
        for note in notes:
            if (note_id := note["note_id"]) in attempted:
                continue
            attempted.add(note_id)
            try:
                data = await self.__deal_extract(
                    self._creator_note_url(note),
                    False,
                    None,
                    True,
                    extract.cookie,
                    extract.proxy,
                )
                if not isinstance(data, dict) or not (
                    summary := self._note_summary(note, data)
                ):
                    errors += 1
                    continue
                result.append(summary)
            except Exception as error:
                self.logging(
                    "Note {0} failed and was skipped: {1}".format(
                        note_id, type(error).__name__
                    ),
                    WARNING,
                )
                errors += 1
        return result, errors

    async def _creator_batch(
        self,
        extract: CreatorBatchParams,
    ) -> CreatorBatchData:
        if not (identity := self._creator_identity(extract.url)):
            return CreatorBatchData(
                message="Fetched 0 notes, 0 errors; invalid creator URL or user_id",
                params=extract,
                data=[],
                next_cursor=None,
            )
        user_id, profile_url = identity
        html = await self.html.request_url(
            profile_url,
            cookie=extract.cookie,
            proxy=extract.proxy,
        )
        try:
            initial_notes = UserPosted.extract_initial_notes(
                self.convert.initial_state(html)
            )
        except Exception as error:
            self.logging(
                "Failed to parse creator profile: {0}".format(type(error).__name__),
                WARNING,
            )
            initial_notes = []

        result = []
        errors = 0
        attempted = set()
        pages = 0
        cursor = extract.cursor
        next_cursor = None
        feed_failed = False

        if not cursor and initial_notes:
            page = initial_notes[: extract.page_size]
            summaries, page_errors = await self._resolve_creator_notes(
                page, extract, attempted
            )
            result.extend(summaries)
            errors += page_errors
            pages += 1
            if len(page) == extract.page_size:
                cursor = page[-1]["note_id"]
                next_cursor = cursor

        while pages < extract.max_pages and (cursor or not initial_notes):
            try:
                response = await UserPosted(
                    self.manager,
                    {
                        "num": extract.page_size,
                        "cursor": cursor,
                        "user_id": user_id,
                        "image_formats": "jpg,webp,avif",
                    },
                    extract.cookie,
                    extract.proxy,
                ).get_data()
            except Exception as error:
                self.logging(
                    "Failed to fetch creator feed page: {0}".format(
                        type(error).__name__
                    ),
                    WARNING,
                )
                feed_failed = True
                next_cursor = cursor or None
                break
            if isinstance(response, dict) and response.get("success") is False:
                feed_failed = True
                next_cursor = cursor or None
                break
            page, following_cursor = UserPosted.extract_api_page(response)
            if not page:
                next_cursor = None
                break
            summaries, page_errors = await self._resolve_creator_notes(
                page[: extract.page_size], extract, attempted
            )
            result.extend(summaries)
            errors += page_errors
            pages += 1
            next_cursor = following_cursor
            if not following_cursor or following_cursor == cursor:
                break
            cursor = following_cursor

        message = f"Fetched {len(result)} notes, {errors} errors"
        if feed_failed:
            message += "; creator feed page failed"
        return CreatorBatchData(
            message=message,
            params=extract,
            data=result,
            next_cursor=next_cursor,
        )

    # @staticmethod
    # Vendor patch (creator-batch streaming):
    #
    # The synchronous `/xhs/creator/batch` endpoint buffers the entire
    # result before responding — for large authors (200+ notes, slow XHS
    # upstream) this can take several minutes. The browser/client
    # fetch can hit a 30s default timeout before the response starts.
    #
    # The streaming endpoint below uses Server-Sent Events format
    # (`data: <json>\n\n`) so the client can:
    #   - See per-page / per-batch progress
    #   - Cancel the underlying fetch without losing partial results
    #   - Drive the UI's "X of Y" progress display
    async def _creator_batch_stream(
        self,
        extract: "CreatorBatchParams",
    ):
        """Generator that yields SSE-formatted events as the creator
        batch progresses. Event shape:
          { kind: "start" | "page" | "done" | "error",
            page?: int, items?: [NoteSummary], saved: int, errors: int,
            message: string, params: CreatorBatchParams,
            next_cursor?: string }
        """
        import json as _json
        from fastapi.responses import StreamingResponse

        def sse(obj: dict) -> bytes:
            return f"data: {_json.dumps(obj, ensure_ascii=False)}\n\n".encode("utf-8")

        try:
            identity = self._creator_identity(extract.url)
            if not identity:
                yield sse({
                    "kind": "error",
                    "page": 0,
                    "items": [],
                    "saved": 0,
                    "errors": 0,
                    "message": "Fetched 0 notes, 0 errors; invalid creator URL or user_id",
                    "params": extract.model_dump() if hasattr(extract, "model_dump") else dict(extract),
                })
                return

            user_id, profile_url = identity
            yield sse({
                "kind": "start",
                "page": 0,
                "items": [],
                "saved": 0,
                "errors": 0,
                "message": f"Fetching creator {user_id}...",
                "params": extract.model_dump() if hasattr(extract, "model_dump") else dict(extract),
            })

            html = await self.html.request_url(
                profile_url, cookie=extract.cookie, proxy=extract.proxy,
            )
            try:
                initial_notes = UserPosted.extract_initial_notes(
                    self.convert.initial_state(html)
                )
            except Exception as error:
                initial_notes = []

            result = []
            errors = 0
            attempted = set()
            pages = 0
            cursor = extract.cursor
            next_cursor = None
            feed_failed = False
            saved = 0

            async def process_page(page_items: list, page_idx: int):
                nonlocal saved, errors, result
                if not page_items:
                    return False
                summaries, page_errors = await self._resolve_creator_notes(
                    page_items[: extract.page_size], extract, attempted
                )
                result.extend(summaries)
                errors += page_errors
                saved += sum(1 for s in summaries if s)
                yield sse({
                    "kind": "page",
                    "page": page_idx,
                    "items": [s.dict() if hasattr(s, "dict") else s for s in summaries],
                    "saved": saved,
                    "errors": errors,
                    "message": f"Page {page_idx}: +{len(summaries)} notes",
                    "params": extract.model_dump() if hasattr(extract, "model_dump") else dict(extract),
                })
                return True

            if not cursor and initial_notes:
                pages = 1
                async for ev in process_page(initial_notes, pages):
                    yield ev
                if len(initial_notes) >= extract.page_size:
                    cursor = initial_notes[-1]["note_id"]

            while pages < extract.max_pages and (cursor or not initial_notes):
                try:
                    response = await UserPosted(
                        self.manager,
                        {
                            "num": extract.page_size,
                            "cursor": cursor,
                            "user_id": user_id,
                            "image_formats": "jpg,webp,avif",
                        },
                        extract.cookie,
                        extract.proxy,
                    ).get_data()
                except Exception:
                    feed_failed = True
                    next_cursor = cursor or None
                    break
                if isinstance(response, dict) and response.get("success") is False:
                    feed_failed = True
                    next_cursor = cursor or None
                    break
                page, following_cursor = UserPosted.extract_api_page(response)
                if not page:
                    next_cursor = None
                    break
                pages += 1
                async for ev in process_page(page, pages):
                    yield ev
                next_cursor = following_cursor
                if not following_cursor or following_cursor == cursor:
                    break
                cursor = following_cursor

            final_message = f"Fetched {len(result)} notes, {errors} errors"
            if feed_failed:
                final_message += "; creator feed page failed"
            yield sse({
                "kind": "done",
                "page": pages,
                "items": [],
                "saved": saved,
                "errors": errors,
                "message": final_message,
                "params": extract.model_dump() if hasattr(extract, "model_dump") else dict(extract),
                "next_cursor": next_cursor,
            })
        except Exception as e:
            yield sse({
                "kind": "error",
                "page": 0,
                "items": [],
                "saved": 0,
                "errors": 1,
                "message": str(e),
                "params": {},
            })

    def handle_creator_batch_stream(self, extract: "CreatorBatchParams"):
        """Route handler that streams SSE events from the creator batch."""
        from fastapi.responses import StreamingResponse

        return StreamingResponse(
            self._creator_batch_stream(extract),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
            },
        )

    # @staticmethod
    # def read_browser_cookie(value: str | int) -> str:
    #     return (
    #         BrowserCookie.get(
    #             value,
    #             domains=[
    #                 "xiaohongshu.com",
    #             ],
    #         )
    #         if value
    #         else ""
    #     )

    async def run_api_server(
        self,
        host="0.0.0.0",
        port=5556,
        log_level="info",
    ):
        api = FastAPI(
            debug=self.VERSION_BETA,
            title="XHS-Downloader",
            version=__VERSION__,
        )
        self.setup_routes(api)
        config = Config(
            api,
            host=host,
            port=port,
            log_level=log_level,
        )
        server = Server(config)
        await server.serve()

    def setup_routes(
        self,
        server: FastAPI,
    ):
        @server.get(
            "/",
            summary=_("跳转至项目 GitHub 仓库"),
            description=_("重定向至项目 GitHub 仓库主页"),
            tags=["API"],
        )
        async def index():
            return RedirectResponse(url=REPOSITORY)

        @server.post(
            "/xhs/detail",
            summary=_("获取作品数据及下载地址"),
            description=_(
                dedent("""
                **参数**:
                        
                - **url**: 小红书作品链接，自动提取，不支持多链接；必需参数
                - **download**: 是否下载作品文件；设置为 true 将会耗费更多时间；可选参数
                - **index**: 下载指定序号的图片文件，仅对图文作品生效；download 参数设置为 false 时不生效；可选参数
                - **cookie**: 请求数据时使用的 Cookie；可选参数
                - **proxy**: 请求数据时使用的代理；可选参数
                - **skip**: 是否跳过存在下载记录的作品；设置为 true 将不会返回存在下载记录的作品数据；可选参数
                """)
            ),
            tags=["API"],
            response_model=ExtractData,
        )
        async def handle(extract: ExtractParams):
            data = None
            url = await self.extract_links(
                extract.url,
            )
            if not url:
                msg = _("提取小红书作品链接失败")
            else:
                if data := await self.__deal_extract(
                    url[0],
                    extract.download,
                    extract.index,
                    not extract.skip,
                    extract.cookie,
                    extract.proxy,
                ):
                    msg = _("获取小红书作品数据成功")
                else:
                    msg = _("获取小红书作品数据失败")
            return ExtractData(message=msg, params=extract, data=data)

        @server.post(
            "/xhs/creator/batch",
            summary=_("分页获取作者作品"),
            description=_(
                dedent("""
                **参数**:

                - **url**: 作者主页链接、@handle 或作者 ID；必需参数
                - **cookie**: 请求数据时使用的 Cookie；可选参数
                - **proxy**: 请求数据时使用的代理；可选参数
                - **cursor**: 从指定游标继续获取；可选参数
                - **page_size**: 每页作品数量；可选参数，默认 18
                - **max_pages**: 单次请求最多获取页数；可选参数，默认 30
                """)
            ),
            tags=["API"],
            response_model=CreatorBatchData,
        )
        async def handle_creator_batch(extract: CreatorBatchParams):
            try:
                return await self._creator_batch(extract)
            except Exception as error:
                self.logging(
                    "Creator feed request failed: {0}".format(type(error).__name__), ERROR
                )
                return CreatorBatchData(
                    message="Fetched 0 notes, 0 errors; creator feed request failed",
                    params=extract,
                    data=[],
                    next_cursor=None,
                )

        @server.post(
            "/xhs/creator/batch/stream",
            summary=_("分页获取作者作品 (SSE 流式)"),
            description=_(
                "Server-Sent Events variant of /xhs/creator/batch. Streams "
                "`data: <json>\\n\\n` events (`{kind: start|page|done|error, ...}`) "
                "as each page is processed — avoids the synchronous 30s+ "
                "timeout for large authors. Recommended for creators with "
                ">20 notes."
            ),
            tags=["API"],
        )
        def handle_creator_batch_stream_route(extract: CreatorBatchParams):
            return self.handle_creator_batch_stream(extract)

    async def run_mcp_server(
        self,
        transport="streamable-http",
        host="0.0.0.0",
        port=5556,
        log_level="INFO",
    ):
        mcp = FastMCP(
            "XHS-Downloader",
            instructions=dedent("""
                本服务器提供两个 MCP 接口，分别用于获取小红书作品信息数据和下载小红书作品文件，二者互不依赖，可独立调用。
                
                支持的作品链接格式：
                - https://www.xiaohongshu.com/explore/...
                - https://www.xiaohongshu.com/discovery/item/...
                - https://xhslink.com/...
                
                get_detail_data
                功能：输入小红书作品链接，返回该作品的信息数据，不会下载文件。
                参数：
                - url（必填）：小红书作品链接
                返回：
                - message：结果提示
                - data：作品信息数据
                
                download_detail
                功能：输入小红书作品链接，下载作品文件，默认不返回作品信息数据。
                参数：
                - url（必填）：小红书作品链接
                - index（选填）：根据用户指定的图片序号（如用户说“下载第1和第3张”时，index应为 [1, 3]），生成由所需图片序号组成的列表；如果用户未指定序号，则该字段为 None
                - return_data（可选）：是否返回作品信息数据；如需返回作品信息数据，设置此参数为 true，默认值为 false
                返回：
                - message：结果提示
                - data：作品信息数据，不需要返回作品信息数据时固定为 None
                """),
            version=__VERSION__,
        )

        @mcp.tool(
            name="get_detail_data",
            description=dedent("""
                功能：输入小红书作品链接，返回该作品的信息数据，不会下载文件。
                
                参数：
                url（必填）：小红书作品链接，格式如：
                - https://www.xiaohongshu.com/explore/...
                - https://www.xiaohongshu.com/discovery/item/...
                - https://xhslink.com/...
                
                返回：
                - message：结果提示
                - data：作品信息数据
                """),
            tags={
                "小红书",
                "XiaoHongShu",
                "RedNote",
            },
            annotations={
                "title": "获取小红书作品信息数据",
                "readOnlyHint": False,
                "destructiveHint": False,
                "idempotentHint": True,
                "openWorldHint": True,
            },
        )
        async def get_detail_data(
            url: Annotated[str, Field(description=_("小红书作品链接"))],
        ) -> dict:
            msg, data = await self.deal_detail_mcp(
                url,
                False,
                None,
            )
            return {
                "message": msg,
                "data": data,
            }

        @mcp.tool(
            name="download_detail",
            description=dedent("""
                功能：输入小红书作品链接，下载作品文件，默认不返回作品信息数据。
                
                参数：
                url（必填）：小红书作品链接，格式如：
                - https://www.xiaohongshu.com/explore/...
                - https://www.xiaohongshu.com/discovery/item/...
                - https://xhslink.com/...
                index（选填）：根据用户指定的图片序号（如用户说“下载第1和第3张”时，index应为 [1, 3]），生成由所需图片序号组成的列表；如果用户未指定序号，则该字段为 None
                return_data（可选）：是否返回作品信息数据；如需返回作品信息数据，设置此参数为 true，默认值为 false
                
                返回：
                - message：结果提示
                - data：作品信息数据，不需要返回作品信息数据时固定为 None
                """),
            tags={
                "小红书",
                "XiaoHongShu",
                "RedNote",
                "Download",
                "下载",
            },
            annotations={
                "title": "下载小红书作品文件，可以返回作品信息数据",
                "readOnlyHint": False,
                "destructiveHint": False,
                "idempotentHint": True,
                "openWorldHint": True,
            },
        )
        async def download_detail(
            url: Annotated[str, Field(description=_("小红书作品链接"))],
            index: Annotated[
                list[str | int] | None,
                Field(default=None, description=_("指定需要下载的图文作品序号")),
            ],
            return_data: Annotated[
                bool,
                Field(default=False, description=_("是否需要返回作品信息数据")),
            ],
        ) -> dict:
            msg, data = await self.deal_detail_mcp(
                url,
                True,
                index,
            )
            match (
                bool(data),
                return_data,
            ):
                case (True, True):
                    return {
                        "message": msg + ", " + _("作品文件下载任务执行完毕"),
                        "data": data,
                    }
                case (True, False):
                    return {
                        "message": _("作品文件下载任务执行完毕"),
                        "data": None,
                    }
                case (False, True):
                    return {
                        "message": msg + ", " + _("作品文件下载任务未执行"),
                        "data": None,
                    }
                case (False, False):
                    return {
                        "message": msg + ", " + _("作品文件下载任务未执行"),
                        "data": None,
                    }
                case _:
                    raise ValueError

        await mcp.run_async(
            transport=transport,
            host=host,
            port=port,
            log_level=log_level,
        )

    async def deal_detail_mcp(
        self,
        url: str,
        download: bool,
        index: list[str | int] | None,
    ):
        data = None
        url = await self.extract_links(
            url,
        )
        if not url:
            msg = _("提取小红书作品链接失败")
        elif data := await self.__deal_extract(
            url[0],
            download,
            index,
            True,
        ):
            msg = _("获取小红书作品数据成功")
        else:
            msg = _("获取小红书作品数据失败")
        return msg, data

    def init_script_server(
        self,
        host="0.0.0.0",
        port=5558,
    ):
        if self.manager.script_server:
            self.run_script_server(host, port)

    async def switch_script_server(
        self,
        host="0.0.0.0",
        port=5558,
        switch: bool = None,
    ):
        if switch is None:
            switch = self.manager.script_server
        if switch:
            self.run_script_server(
                host,
                port,
            )
        else:
            await self.stop_script_server()

    def run_script_server(
        self,
        host="0.0.0.0",
        port=5558,
    ):
        if not self.script:
            self.script = create_task(self._run_script_server(host, port))

    async def _run_script_server(
        self,
        host="0.0.0.0",
        port=5558,
    ):
        async with ScriptServer(self, host, port):
            await Future()

    async def stop_script_server(self):
        if self.script:
            self.script.cancel()
            with suppress(CancelledError):
                await self.script
            self.script = None

    async def _script_server_debug(self):
        await self.switch_script_server(
            switch=self.manager.script_server,
        )

    def logging(self, text, style=INFO):
        logging(
            self.print,
            text,
            style,
        )
