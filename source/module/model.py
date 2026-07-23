from typing import Literal

from pydantic import BaseModel, Field


class ExtractParams(BaseModel):
    url: str
    download: bool = False
    index: list[str | int] | None = None
    cookie: str = None
    proxy: str = None
    skip: bool = False


class ExtractData(BaseModel):
    message: str
    params: ExtractParams
    data: dict | None


class CreatorBatchParams(BaseModel):
    url: str
    cookie: str = ""
    proxy: str = ""
    cursor: str = ""
    page_size: int = Field(default=18, ge=1, le=30)
    max_pages: int = Field(default=30, ge=1, le=100)


class NoteSummary(BaseModel):
    note_id: str
    desc: str
    kind: Literal["images", "video", "livephoto"]
    media_urls: list[str]
    cover_url: str | None = None


class CreatorBatchData(BaseModel):
    message: str
    params: CreatorBatchParams
    data: list[NoteSummary]
    next_cursor: str | None = None
