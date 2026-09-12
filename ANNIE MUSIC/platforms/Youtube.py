import asyncio
import contextlib
import json
import os
import re
import time
from typing import Dict, List, Optional, Tuple, Union

import yt_dlp
from pyrogram.enums import MessageEntityType
from pyrogram.types import Message
from youtubesearchpython.__future__ import VideosSearch

from ANNIEMUSIC.utils.cookie_handler import COOKIE_PATH
from ANNIEMUSIC.utils.database import is_on_off
from ANNIEMUSIC.utils.downloader import download_audio_concurrent, yt_dlp_download
from ANNIEMUSIC.utils.errors import capture_internal_err
from ANNIEMUSIC.utils.formatters import time_to_seconds
from ANNIEMUSIC.utils.tuning import (
    YTDLP_TIMEOUT,
    YOUTUBE_META_MAX,
    YOUTUBE_META_TTL,
)

_cache: Dict[str, Tuple[float, List[Dict]]] = {}
_cache_lock = asyncio.Lock()
_formats_cache: Dict[str, Tuple[float, List[Dict], str]] = {}
_formats_lock = asyncio.Lock()


def _cookiefile_path() -> Optional[str]:
    path = str(COOKIE_PATH)
    try:
        if path and os.path.exists(path) and os.path.getsize(path) > 0:
            return path
    except Exception:
        pass
    return None


def _cookies_args() -> List[str]:
    p = _cookiefile_path()
    return ["--cookies", p] if p else []


async def _exec_proc(*args: str) -> Tuple[bytes, bytes]:
    proc = await asyncio.create_subprocess_exec(
        *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    try:
        return await asyncio.wait_for(proc.communicate(), timeout=YTDLP_TIMEOUT)
    except asyncio.TimeoutError:
        with contextlib.suppress(Exception):
            proc.kill()
        return b"", b"timeout"


@capture_internal_err
async def cached_youtube_search(query: str) -> List[Dict]:
    key = f"q:{query}"
    now = time.time()
    async with _cache_lock:
        if key in _cache:
            ts, val = _cache[key]
            if now - ts < YOUTUBE_META_TTL:
                print(f"[DEBUG] Using cached result for query: {query}")
                return val
            _cache.pop(key, None)
        if len(_cache) > YOUTUBE_META_MAX:
            _cache.clear()
    try:
        print(f"[DEBUG] Performing YouTube search via API for: {query}")
        data = await VideosSearch(query, limit=1).next()
        result = data.get("result", [])
    except Exception as e:
        print(f"[ERROR] VideosSearch failed: {e}")
        result = []
    if result:
        async with _cache_lock:
            _cache[key] = (now, result)
    return result


class YouTubeAPI:
    def __init__(self) -> None:
        self.base_url = "https://www.youtube.com/watch?v="
        self.playlist_url = "https://youtube.com/playlist?list="
        self._url_pattern = re.compile(r"(?:youtube\.com|youtu\.be)")

    def _prepare_link(
        self, link: str, videoid: Union[str, bool, None] = None
    ) -> str:
        if isinstance(videoid, str) and videoid.strip():
            link = self.base_url + videoid.strip()
        if "youtu.be" in link:
            link = self.base_url + link.split("/")[-1].split("?")[0]
        elif "youtube.com/shorts/" in link or "youtube.com/live/" in link:
            link = self.base_url + link.split("/")[-1].split("?")[0]
        return link.split("&")[0]

    @capture_internal_err
    async def exists(
        self, link: str, videoid: Union[str, bool, None] = None
    ) -> bool:
        return bool(self._url_pattern.search(self._prepare_link(link, videoid)))

    @capture_internal_err
    async def url(self, message: Message) -> Optional[str]:
        msgs = [message] + (
            [message.reply_to_message] if message.reply_to_message else []
        )
        for msg in msgs:
            text = msg.text or msg.caption or ""
            entities = msg.entities or msg.caption_entities or []
            for ent in entities:
                if ent.type == MessageEntityType.URL:
                    return text[ent.offset : ent.offset + ent.length]
                if ent.type == MessageEntityType.TEXT_LINK:
                    return ent.url
        return None

    @capture_internal_err
    async def _fetch_video_info(
        self, query: str, *, use_cache: bool = True
    ) -> Optional[Dict]:
        q = self._prepare_link(query)
        try:
            print(f"[DEBUG] Trying YouTube API search for query: {q}")
            if use_cache and not q.startswith("http"):
                res = await cached_youtube_search(q)
                if res:
                    print(f"[DEBUG] Found results from cache or API for: {q}")
                    return res[0]
            data = await VideosSearch(q, limit=1).next()
            result = data.get("result", [])
            if result:
                print(f"[DEBUG] Found {len(result)} result(s) from VideosSearch for: {q}")
                return result[0]
            print(f"[WARN] No results found for {q} using VideosSearch.")
        except Exception as e:
            print(f"[ERROR] VideosSearch failed for {q}: {e}")
        return None

    @capture_internal_err
    async def is_live(self, link: str) -> bool:
        prepared = self._prepare_link(link)
        print(f"[DEBUG] Checking if video is live: {prepared}")
        stdout, _ = await _exec_proc(
            "yt-dlp", *(_cookies_args()), "--dump-json", prepared
        )
        if not stdout:
            return False
        try:
            info = json.loads(stdout.decode())
            return bool(info.get("is_live"))
        except json.JSONDecodeError:
            return False

    @capture_internal_err
    async def details(
        self, link: str, videoid: Union[str, bool, None] = None
    ) -> Tuple[str, Optional[str], int, str, str]:
        print(f"[DEBUG] Fetching details for: {link}")
        info = await self._fetch_video_info(self._prepare_link(link, videoid))
        if not info:
            raise ValueError("Video not found")
        dt = info.get("duration")
        ds = int(time_to_seconds(dt)) if dt else 0
        thumb = (
            info.get("thumbnail")
            or info.get("thumbnails", [{}])[0].get("url", "")
        ).split("?")[0]
        return info.get("title", ""), dt, ds, thumb, info.get("id", "")

    @capture_internal_err
    async def track(
        self, link: str, videoid: Union[str, bool, None] = None
    ) -> Tuple[Dict, str]:
        prepared = self._prepare_link(link, videoid)
        print(f"[DEBUG] Fetching track info for: {prepared}")
        try:
            info = await self._fetch_video_info(prepared)
            if not info:
                raise ValueError("Track not found via API")
        except Exception as api_err:
            print(f"[WARN] API fetch failed for {prepared}: {api_err}")
            print("[DEBUG] Falling back to yt-dlp for track info...")
            stdout, stderr = await _exec_proc(
                "yt-dlp", *(_cookies_args()), "--dump-json", prepared
            )
            if not stdout:
                print(f"[ERROR] yt-dlp fallback failed: {stderr.decode()}")
                raise ValueError("Track not found (yt-dlp fallback)")
            try:
                info = json.loads(stdout.decode())
            except Exception as parse_err:
                print(f"[ERROR] Failed to parse yt-dlp output: {parse_err}")
                raise ValueError("Invalid yt-dlp response")

        thumb = (
            info.get("thumbnail")
            or info.get("thumbnails", [{}])[0].get("url", "")
        ).split("?")[0]
        details = {
            "title": info.get("title", ""),
            "link": info.get("webpage_url", prepared),
            "vidid": info.get("id", ""),
            "duration_min": info.get("duration")
            if isinstance(info.get("duration"), str)
            else None,
            "thumb": thumb,
        }
        print(f"[DEBUG] Final track details: {details}")
        return details, info.get("id", "")

    @capture_internal_err
    async def download(
        self,
        link: str,
        mystic,
        *,
        video: Union[bool, str, None] = None,
        videoid: Union[str, bool, None] = None,
        songaudio: Union[bool, str, None] = None,
        songvideo: Union[bool, str, None] = None,
        format_id: Union[bool, str, None] = None,
        title: Union[bool, str, None] = None,
    ) -> Union[Tuple[str, Optional[bool]], Tuple[None, None]]:
        print(f"[DEBUG] Starting download for: {link}")
        link = self._prepare_link(link, videoid)

        if songvideo:
            print("[DEBUG] Downloading as song video")
            p = await yt_dlp_download(link, type="song_video", format_id=format_id, title=title)
            return (p, True) if p else (None, None)

        if songaudio:
            print("[DEBUG] Downloading as song audio")
            p = await yt_dlp_download(link, type="song_audio", format_id=format_id, title=title)
            return (p, True) if p else (None, None)

        if video:
            print("[DEBUG] Downloading video")
            if await self.is_live(link):
                status, stream_url = await self.video(link)
                if status == 1:
                    return stream_url, None
                raise ValueError("Unable to fetch live stream link")
            if await is_on_off(1):
                p = await yt_dlp_download(link, type="video")
                return (p, True) if p else (None, None)
            stdout, _ = await _exec_proc(
                "yt-dlp",
                *(_cookies_args()),
                "-g",
                "-f",
                "best[height<=?720][width<=?1280]",
                link,
            )
            if stdout:
                return stdout.decode().split("\n")[0], None
            return None, None

        print("[DEBUG] Downloading audio only")
        p = await download_audio_concurrent(link)
        return (p, True) if p else (None, None)
