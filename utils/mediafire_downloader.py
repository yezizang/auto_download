#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MediaFire 下载器模块
====================
使用 Playwright 绕过 Cloudflare 获取下载链接，然后流式下载。

流程:
  1. Playwright 加载文件页面 → BS4 解析 → 提取 a#downloadButton 的 href
  2. 流式下载（断点续传 + 自动重试）

反爬策略: Playwright 浏览器指纹绕过 Cloudflare, 下载阶段代理→直连自动回退

公开 API:
  download(url, output_dir, *, progress_callback, stop_event) -> str | None
"""

import os
import re
import time
from threading import Event
from typing import Callable
from urllib.parse import unquote_plus, urlparse

import requests
from bs4 import BeautifulSoup

# =============================================================================
# 常量
# =============================================================================

CHUNK_SIZE: int = 2 * 1024 * 1024  # 2 MB
MAX_RETRIES: int = 5
CONNECT_TIMEOUT: float = 15.0
READ_TIMEOUT: float = 120.0
PROXY: str | None = (
    os.environ.get("ALL_PROXY")
    or os.environ.get("HTTPS_PROXY")
    or "socks5h://127.0.0.1:7891"
)

_UA: str = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

_PAGE_TIMEOUT_MS: int = 30_000


# =============================================================================
# 内部工具
# =============================================================================


def _build_session(*, use_proxy: bool = True) -> requests.Session:
    """创建带反爬配置的 Session。"""
    s = requests.Session()
    s.trust_env = False
    if use_proxy and PROXY:
        s.proxies = {"http": PROXY, "https": PROXY}
    s.headers.update(
        {
            "User-Agent": _UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }
    )
    return s


def _try_download_stream(url: str, headers: dict) -> requests.Response:
    """下载流式请求，代理失败则直连重试。"""
    for use_proxy in (True, False):
        sess = _build_session(use_proxy=use_proxy)
        try:
            return sess.get(
                url,
                headers=headers,
                stream=True,
                timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
            )
        except Exception:
            if not use_proxy:
                raise
            continue
    raise RuntimeError("unreachable")


def _fetch_page(url: str) -> str | None:
    """用 Playwright 获取页面 HTML（绕过 Cloudflare）。"""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return None

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            try:
                page = browser.new_page()
                page.goto(url, wait_until="networkidle", timeout=_PAGE_TIMEOUT_MS)
                return page.content()
            finally:
                browser.close()
    except Exception:
        return None


def _extract_download_url(html: str) -> str | None:
    """从 MediaFire 页面提取下载直链。"""
    soup = BeautifulSoup(html, "html.parser")
    btn = soup.find("a", {"id": "downloadButton"})
    if btn and btn.get("href"):
        return btn["href"]
    return None


def _extract_filename(url: str) -> str:
    """从下载 URL 路径提取文件名。"""
    parsed = urlparse(url)
    path = unquote_plus(parsed.path)
    name = path.rstrip("/").rsplit("/", 1)[-1]
    return name or "download"


# =============================================================================
# 公开 API
# =============================================================================


def download(
    url: str,
    output_dir: str,
    *,
    progress_callback: Callable[[dict], None] | None = None,
    stop_event: Event | None = None,
) -> str | None:
    """
    从 mediafire.com 下载文件。

    参数:
        url:               MediaFire 文件页面链接
        output_dir:        保存目录
        progress_callback: 进度回调 → {status, filename, downloaded, total, speed, percent}
        stop_event:        取消事件

    返回:
        成功返回文件路径，失败返回 None
    """
    stop = stop_event or Event()

    def _notify(
        status: str,
        filename: str,
        downloaded: int,
        total: int | None,
        speed: float,
        percent: float,
    ) -> None:
        if progress_callback:
            progress_callback(
                {
                    "status": status,
                    "filename": filename,
                    "downloaded": downloaded,
                    "total": total,
                    "speed": speed,
                    "percent": percent,
                }
            )

    # ---- Step 1: Playwright 获取页面，提取下载链接 ----
    _notify("connecting", "Loading page via browser...", 0, None, 0, 0)

    html = _fetch_page(url)
    if not html:
        _notify("failed", "Failed to load page (Cloudflare/network)", 0, None, 0, 0)
        return None

    download_url = _extract_download_url(html)
    if not download_url:
        _notify("failed", "Download link not found on page", 0, None, 0, 0)
        return None

    if stop.is_set():
        return None

    # ---- Step 2: 解析文件名 ----
    filename = _extract_filename(download_url)

    if stop.is_set():
        return None

    # ---- Step 3: 确定保存路径 ----
    filepath = os.path.join(output_dir, filename)

    if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
        size = os.path.getsize(filepath)
        _notify("completed", filename, size, size, 0, 100)
        return filepath

    # ---- Step 4: 流式下载 ----
    _notify("downloading", filename, 0, None, 0, 0)
    tmp = f"{filepath}.part"

    for attempt in range(1, MAX_RETRIES + 1):
        if stop.is_set():
            return None

        part_size = 0
        req_headers: dict[str, str] = {}
        if os.path.exists(tmp):
            part_size = os.path.getsize(tmp)
            if part_size > 0:
                req_headers["Range"] = f"bytes={part_size}-"

        try:
            resp = _try_download_stream(download_url, req_headers)

            if part_size > 0 and resp.status_code not in (206, 200):
                part_size = 0
                open(tmp, "wb").close()
                resp = _try_download_stream(download_url, {})

            if resp.status_code not in (200, 206):
                if attempt < MAX_RETRIES:
                    time.sleep(2**attempt)
                continue

            # 从 Content-Disposition 覆盖文件名
            cd = resp.headers.get("content-disposition", "")
            if cd:
                m = re.search(r'filename\*?=(?:UTF-8\'\')?["\']?([^"\';]+)', cd, re.I)
                if m:
                    filename = m.group(1).strip("\"' ")
                    filepath = os.path.join(output_dir, filename)
                    tmp = f"{filepath}.part"

            remote_size = None
            cl = resp.headers.get("content-length")
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
                for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
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

            if remote_size and os.path.getsize(tmp) != remote_size:
                if attempt < MAX_RETRIES:
                    time.sleep(2**attempt)
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
                time.sleep(2**attempt)
        except Exception:
            if attempt < MAX_RETRIES:
                time.sleep(2**attempt)

    _notify("failed", filename, 0, None, 0, 0)
    return None
