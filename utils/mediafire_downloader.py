#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MediaFire 下载器 — 从文件页面提取 a#downloadButton 直链后下载。

公开 API:
  download(url, output_dir, *, progress_callback, stop_event) -> str | None
"""

import os
import re
import time
from threading import Event
from typing import Callable
from urllib.parse import urlparse, unquote

import requests
from bs4 import BeautifulSoup

from utils.logger import error as _log_error

CHUNK_SIZE = 2 * 1024 * 1024  # 2 MB
MAX_RETRIES = 3
TIMEOUT = (15, 120)

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)


def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": _UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
    })
    # 尊重环境代理
    proxy = os.environ.get("ALL_PROXY") or os.environ.get("HTTPS_PROXY")
    if proxy:
        s.proxies = {"http": proxy, "https": proxy}
    return s


def _extract_download_url(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    btn = soup.find("a", id="downloadButton")
    if btn and btn.get("href"):
        return btn["href"]
    return None


def _extract_filename(url: str) -> str:
    parsed = urlparse(url)
    path = unquote(parsed.path)
    return path.rstrip("/").rsplit("/", 1)[-1] or "download"


def download(
    url: str,
    output_dir: str,
    *,
    progress_callback: Callable[[dict], None] | None = None,
    stop_event: Event | None = None,
) -> str | None:
    stop = stop_event or Event()

    def _notify(status: str, filename: str, downloaded: int = 0,
                total: int | None = None, speed: float = 0, percent: float = 0):
        if progress_callback:
            progress_callback(dict(
                status=status, filename=filename, downloaded=downloaded,
                total=total, speed=speed, percent=percent))

    # ---- Step 1: GET 文件页面 (shared session 贯穿全文) ----
    session = _make_session()
    _notify("connecting", "", 0, None, 0, 0)

    try:
        resp = session.get(url, timeout=TIMEOUT)
        resp.raise_for_status()
    except Exception as e:
        _notify("failed", str(e)[:120])
        _log_error(f"mediafire: failed to fetch page: {url}", exc=e)
        return None

    download_url = _extract_download_url(resp.text)
    if not download_url:
        _notify("failed", "Download link not found")
        _log_error(f"mediafire: download link not found: {url}")
        return None

    if stop.is_set():
        return None

    # ---- Step 2: 解析文件名 ----
    filename = _extract_filename(download_url)
    filepath = os.path.join(output_dir, filename)

    if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
        size = os.path.getsize(filepath)
        _notify("completed", filename, size, size, percent=100)
        return filepath

    # ---- Step 3: 流式下载 ----
    _notify("downloading", filename)
    tmp = f"{filepath}.part"
    headers = {"Referer": url}

    for attempt in range(1, MAX_RETRIES + 1):
        if stop.is_set():
            return None

        part_size = os.path.getsize(tmp) if os.path.exists(tmp) else 0
        if part_size > 0:
            headers["Range"] = f"bytes={part_size}-"

        try:
            dl_resp = session.get(download_url, headers=headers, stream=True, timeout=TIMEOUT)

            # 服务器不支持断点续传就重头来
            if part_size > 0 and dl_resp.status_code not in (206, 200):
                part_size = 0
                open(tmp, "wb").close()
                dl_resp = session.get(download_url, headers={"Referer": url},
                                      stream=True, timeout=TIMEOUT)

            if dl_resp.status_code not in (200, 206):
                if attempt < MAX_RETRIES:
                    time.sleep(2 ** attempt)
                continue

            # 从 Content-Disposition 覆盖文件名
            cd = dl_resp.headers.get("content-disposition", "")
            if cd:
                m = re.search(r'filename\*?=(?:UTF-8\'\')?["\']?([^"\';]+)', cd, re.I)
                if m:
                    filename = m.group(1).strip("\"' ")
                    filepath = os.path.join(output_dir, filename)
                    tmp = f"{filepath}.part"

            remote_size = None
            cl = dl_resp.headers.get("content-length")
            if cl:
                try:
                    remote_size = int(cl) + part_size
                except ValueError:
                    pass

            mode = "ab" if part_size else "wb"
            t0 = time.perf_counter()
            downloaded = part_size
            last_rpt = 0.0

            with open(tmp, mode) as f:
                for chunk in dl_resp.iter_content(chunk_size=CHUNK_SIZE):
                    if stop.is_set():
                        return None
                    if not chunk:
                        continue
                    f.write(chunk)
                    downloaded += len(chunk)

                    now = time.perf_counter()
                    if now - last_rpt >= 0.5 or downloaded == remote_size:
                        elap = now - t0
                        spd = (downloaded - part_size) / elap if elap > 0 else 0
                        pct = (downloaded / remote_size * 100) if remote_size else 0
                        _notify("downloading", filename, downloaded, remote_size, spd, pct)
                        last_rpt = now

            # 校验大小
            if remote_size and os.path.getsize(tmp) != remote_size:
                if attempt < MAX_RETRIES:
                    time.sleep(2 ** attempt)
                continue

            if os.path.exists(filepath):
                os.remove(filepath)
            os.rename(tmp, filepath)

            elap = time.perf_counter() - t0
            avg = (downloaded - part_size) / elap if elap > 0 else 0
            _notify("completed", filename, downloaded, remote_size or downloaded, avg, 100)
            return filepath

        except requests.RequestException:
            if attempt < MAX_RETRIES:
                time.sleep(2 ** attempt)
        except Exception:
            if attempt < MAX_RETRIES:
                time.sleep(2 ** attempt)

    _notify("failed", filename)
    _log_error(f"mediafire: download failed after retries: {download_url} → {filename}")
    return None
