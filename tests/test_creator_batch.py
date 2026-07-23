import tempfile
import unittest
from unittest.mock import AsyncMock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from source import XHS
from source.application.user_posted import UserPosted
from source.module import CreatorBatchParams


class CreatorBatchTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        XHS._XHS__INSTANCE = None
        self.xhs = XHS(work_path=self.temp_dir.name, max_retry=0)

    async def asyncTearDown(self):
        await self.xhs.close()
        XHS._XHS__INSTANCE = None
        self.temp_dir.cleanup()

    async def test_batch_skips_failed_note(self):
        html = """
        <script>window.__INITIAL_STATE__={"user":{"notes":{"_rawValue":[[
          {"id":"note-one","xsecToken":"token-one","noteCard":{"cover":{"urlDefault":"https://img/one"}}},
          {"id":"note-two","xsecToken":"token-two","noteCard":{"cover":{"urlDefault":"https://img/two"}}}
        ]]}}}</script>
        """
        self.xhs.html.request_url = AsyncMock(return_value=html)
        self.xhs._XHS__deal_extract = AsyncMock(
            side_effect=[
                {
                    "作品ID": "note-one",
                    "作品描述": "first",
                    "作品类型": "图文",
                    "下载地址": ["https://img/one-full"],
                    "动图地址": [None],
                },
                RuntimeError("note unavailable"),
            ]
        )

        response = await self.xhs._creator_batch(
            CreatorBatchParams(
                url="creator-id",
                page_size=2,
                max_pages=1,
            )
        )

        self.assertEqual(response.message, "Fetched 1 notes, 1 errors")
        self.assertEqual(response.next_cursor, "note-two")
        self.assertEqual(response.data[0].note_id, "note-one")
        self.assertEqual(response.data[0].kind, "images")
        self.assertEqual(response.data[0].cover_url, "https://img/one")

    async def test_creator_request_generates_anonymous_signing_cookies(self):
        request = UserPosted(
            self.xhs.manager,
            {
                "num": 18,
                "cursor": "",
                "user_id": "creator-id",
                "image_formats": "jpg,webp,avif",
            },
        )

        headers = {key.lower(): value for key, value in request.get_headers().items()}
        self.assertIn("a1", request.cookies)
        self.assertIn("webId", request.cookies)
        self.assertIn("x-s", headers)
        self.assertIn("x-rap-param", headers)

    async def test_openapi_and_soft_failure_contract(self):
        api = FastAPI()
        self.xhs.setup_routes(api)
        with TestClient(api) as client:
            openapi = client.get("/openapi.json").json()
            response = client.post(
                "/xhs/creator/batch",
                json={"url": "not a valid creator"},
            )

        self.assertIn("/xhs/creator/batch", openapi["paths"])
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["data"], [])
        self.assertIsNone(body["next_cursor"])
        self.assertEqual(body["params"]["page_size"], 18)
        self.assertEqual(body["params"]["max_pages"], 30)
        self.assertIsNone(
            self.xhs._creator_identity(
                "https://evil.example/?next=https://www.xiaohongshu.com/user/profile/id"
            )
        )

    async def test_video_requires_profile_cover(self):
        data = {
            "作品ID": "video-note",
            "作品描述": "video",
            "作品类型": "视频",
            "下载地址": ["https://video/full"],
            "动图地址": [None],
        }
        summary = self.xhs._note_summary(
            {"note_id": "video-note", "cover_url": "https://img/cover"}, data
        )

        self.assertEqual(summary.kind, "video")
        self.assertEqual(summary.cover_url, "https://img/cover")
        self.assertIsNone(
            self.xhs._note_summary(
                {"note_id": "video-note", "cover_url": None}, data
            )
        )


if __name__ == "__main__":
    unittest.main()
