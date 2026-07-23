from http.cookies import SimpleCookie
from typing import TYPE_CHECKING, Any

from httpx import AsyncClient
from xhshow import Xhshow

from ..module import retry, sleep_time

if TYPE_CHECKING:
    from ..module import Manager

__all__ = ["UserPosted"]


class UserPosted:
    URL = "https://edith.xiaohongshu.com/api/sns/web/v1/user_posted"
    encipher = Xhshow()

    def __init__(
        self,
        manager: "Manager",
        params: dict,
        cookies: str = None,
        proxy: str = None,
    ):
        self.url = self.URL
        self.params = params
        self.headers = manager.blank_headers | {
            "referer": "https://www.xiaohongshu.com/",
        }
        self.client = manager.request_client
        self.cookies = self.get_cookie(cookies)
        self.retry = manager.retry
        self.timeout = manager.timeout
        self.proxy = proxy

    def get_cookie(self, cookies: str = None) -> dict:
        if cookies:
            parsed = SimpleCookie()
            parsed.load(cookies)
            values = {key: morsel.value for key, morsel in parsed.items()}
        else:
            values = dict(self.client.cookies)
        if not values.get("a1"):
            values["a1"] = self.encipher.generate_a1()
        if not values.get("webId"):
            values["webId"] = self.encipher.generate_web_id(values["a1"])
        self.client.cookies.update(values)
        self.headers["cookie"] = "; ".join(
            f"{key}={value}" for key, value in values.items()
        )
        return values

    @retry
    async def get_data(self) -> dict:
        headers = self.get_headers()
        if self.proxy:
            async with AsyncClient(
                headers=headers,
                cookies=self.cookies,
                proxy=self.proxy,
                follow_redirects=True,
                verify=False,
                timeout=self.timeout,
            ) as client:
                response = await client.get(self.url, params=self.params)
        else:
            response = await self.client.get(
                self.url,
                params=self.params,
                headers=headers,
            )
        await sleep_time()
        response.raise_for_status()
        return response.json()

    def get_headers(self) -> dict:
        headers = self.encipher.sign_headers_get(
            uri=self.url,
            cookies=self.cookies,
            params=self.params,
            x_rap=True,
            user_id=self.params["user_id"],
        )
        return self.headers | headers

    @classmethod
    def extract_initial_notes(cls, state: dict) -> list[dict]:
        # Vendor patch: XHS's `__INITIAL_STATE__` exposes the resolved
        # `user.notes` array directly (one entry per sub-tab, the first
        # is the active "笔记" tab). The earlier "_rawValue" suffix was
        # an Immer-draft guess that never matched the rendered HTML.
        # See plans/260723-1056-xhs-downloader-integration/research/
        #     xhs-engine-spike-report.md §3
        raw = cls._path(state, "user", "notes")
        if isinstance(raw, dict):
            raw = raw.get(0) or raw.get("0") or next(iter(raw.values()), [])
        elif isinstance(raw, list) and raw and isinstance(raw[0], list):
            raw = raw[0]
        return cls._normalize_notes(raw)

    @classmethod
    def extract_api_page(cls, response: dict) -> tuple[list[dict], str | None]:
        data = response.get("data", response) if isinstance(response, dict) else {}
        if not isinstance(data, dict):
            return [], None
        notes = cls._normalize_notes(data.get("notes", []))
        next_cursor = data.get("cursor") if data.get("has_more") else None
        return notes, str(next_cursor) if next_cursor else None

    @classmethod
    def _normalize_notes(cls, items: Any) -> list[dict]:
        if not isinstance(items, list):
            return []
        result = []
        for item in items:
            if not isinstance(item, dict):
                continue
            card = item.get("noteCard") or item.get("note_card") or item
            note_id = cls._first(
                item,
                "id",
                "noteId",
                "note_id",
            ) or cls._first(card, "noteId", "note_id", "id")
            if not note_id:
                continue
            token = cls._first(item, "xsecToken", "xsec_token") or cls._first(
                card, "xsecToken", "xsec_token"
            )
            cover = card.get("cover", {}) if isinstance(card, dict) else {}
            cover_url = (
                cls._first(cover, "urlDefault", "url_default", "url")
                if isinstance(cover, dict)
                else None
            )
            result.append(
                {
                    "note_id": str(note_id),
                    "xsec_token": str(token or ""),
                    "cover_url": str(cover_url) if cover_url else None,
                }
            )
        return result

    @staticmethod
    def _path(data: dict, *keys: str):
        current = data
        for key in keys:
            if not isinstance(current, dict):
                return None
            current = current.get(key)
        return current

    @staticmethod
    def _first(data: dict, *keys: str):
        if not isinstance(data, dict):
            return None
        return next((data[key] for key in keys if data.get(key)), None)
