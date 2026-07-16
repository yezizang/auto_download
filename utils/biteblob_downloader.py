#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BiteBlob 下载器模块
===================
从 Information 页面提取 downloadButton 的 onclick 下载路径，拼接后下载。

流程:
  1. GET Information 页面 → BS4 解析 → 提取 onclick 中的 /Download/ 路径
  2. 拼接完整下载 URL
  3. 流式下载（断点续传 + 自动重试）

反爬策略: Session 保持 cookie, UA 伪装, 代理→直连自动回退

公开 API:
  download(url, output_dir, *, progress_callback, stop_event) -> str | None
"""

import os
import re
import time
from threading import Event
from typing import Callable
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from utils.logger import error as _log_error

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


# =============================================================================
# 内部工具
# =============================================================================


def _build_session(*, use_proxy: bool = True) -> requests.Session:
    """创建带反爬配置的 Session。use_proxy=False 时直连。"""
    s = requests.Session()
    s.trust_env = False  # 不使用系统/环境代理，完全由代码控制
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


def _try_fetch(url: str) -> requests.Response:
    """先走代理，代理失败则直连重试。"""
    for use_proxy in (True, False):
        sess = _build_session(use_proxy=use_proxy)
        try:
            return sess.get(url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
        except Exception:
            if not use_proxy:
                raise
            continue
    raise RuntimeError("unreachable")


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


def _extract_download_path(html: str) -> str | None:
    """从 Information 页面提取下载路径。

    页面中的按钮结构:
      <button id="downloadButton"
              onclick="location.href='/Download/<id>/#<filename>'">
    """
    soup = BeautifulSoup(html, "html.parser")
    btn = soup.find("button", {"id": "downloadButton"})
    if not btn or "onclick" not in btn.attrs:
        return None

    onclick: str = btn["onclick"]
    # onclick 格式: location.href='/Download/...'
    # 去掉 location.href=' 前缀（16 字符）和末尾引号
    if onclick.startswith("location.href="):
        path = onclick[15:]  # 去掉 "location.href="
        path = path.strip("'\"").lstrip("'\"").rstrip("'\"")
        return path

    # fallback: 正则提取
    m = re.search(r"/Download/[^'\"]+", onclick)
    return m.group(0) if m else None


def _extract_filename(path: str) -> str:
    """从下载路径的 # 片段提取文件名。"""
    if "#" in path:
        fragment = path.rsplit("#", 1)[-1]
        # 去掉 @mention 噪音 → "file.txt @user" → "file.txt"
        name = re.sub(r"\s*@\S+", "", fragment).strip()
        if name:
            return name
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
    从 biteblob.com 下载文件。

    参数:
        url:               Information 页面链接
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

    # ---- Step 1: 获取 Information 页面，提取下载路径 ----
    _notify("connecting", "Fetching page...", 0, None, 0, 0)
    try:
        resp = _try_fetch(url)
        resp.raise_for_status()
    except Exception as e:
        _notify("failed", str(e)[:120], 0, None, 0, 0)
        _log_error(f"biteblob: failed to fetch page: {url}", exc=e)
        return None

    dl_path = _extract_download_path(resp.text)
    if not dl_path:
        _notify("failed", "Download link not found on page", 0, None, 0, 0)
        _log_error(f"biteblob: download link not found on page: {url}")
        return None

    download_url = urljoin("https://biteblob.com", dl_path)

    if stop.is_set():
        return None

    # ---- Step 2: 解析文件名 ----
    filename = _extract_filename(dl_path)

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
            resp2 = _try_download_stream(download_url, req_headers)

            # 断点续传不支持 → 重头开始
            if part_size > 0 and resp2.status_code not in (206, 200):
                part_size = 0
                open(tmp, "wb").close()
                resp2 = _try_download_stream(download_url, {})

            if resp2.status_code not in (200, 206):
                if attempt < MAX_RETRIES:
                    time.sleep(2**attempt)
                continue

            # 从 Content-Disposition 覆盖文件名
            cd = resp2.headers.get("content-disposition", "")
            if cd:
                m = re.search(r'filename\*?=(?:UTF-8\'\')?["\']?([^"\';]+)', cd, re.I)
                if m:
                    filename = m.group(1).strip("\"' ")
                    filepath = os.path.join(output_dir, filename)
                    tmp = f"{filepath}.part"

            # 总大小
            remote_size = None
            cl = resp2.headers.get("content-length")
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
                for chunk in resp2.iter_content(chunk_size=CHUNK_SIZE):
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
                        _notify(
                            "downloading", filename, downloaded, remote_size, spd, pct
                        )
                        last_rpt = now

            # 校验完整性
            if remote_size and os.path.getsize(tmp) != remote_size:
                if attempt < MAX_RETRIES:
                    time.sleep(2**attempt)
                continue

            if os.path.exists(filepath):
                os.remove(filepath)
            os.rename(tmp, filepath)

            elap = time.perf_counter() - t0
            avg = (downloaded - part_size) / elap if elap > 0 else 0
            _notify(
                "completed", filename, downloaded, remote_size or downloaded, avg, 100
            )
            return filepath

        except requests.RequestException:
            if attempt < MAX_RETRIES:
                time.sleep(2**attempt)
        except Exception:
            if attempt < MAX_RETRIES:
                time.sleep(2**attempt)

    _notify("failed", filename, 0, None, 0, 0)
    _log_error(f"biteblob: download failed after retries: {download_url} → {filename}")
    return None
