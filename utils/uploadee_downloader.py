#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
upload.ee 文件下载器
===================
功能：
  - 传入 upload.ee 文件页 URL，自动解析真实下载链接
  - 分块流式下载（不占满内存）
  - 断点续传
  - 自动重试（指数退避 + stop_event 可中断）
  - 进度回调

公开 API：
  downloader(url, output_dir, *, progress_callback, stop_event) -> str | None
"""

import re
import time
import requests
from os import getcwd, rename
from os import path as os_path
from typing import Callable
from threading import Event
from bs4 import BeautifulSoup

from utils.logger import error as _log_error

# =============================================================================
# 常量
# =============================================================================

CHUNK_SIZE: int = 2 * 1024 * 1024  # 2 MB
MAX_RETRIES: int = 5
CONNECT_TIMEOUT: float = 15.0
READ_TIMEOUT: float = 60.0
RETRY_BACKOFF_BASE: float = 3.0  # 缩短退避基数，避免长时间卡死

PROXY: str = "socks5h://127.0.0.1:7891"

# 带代理的共享 Session（复用连接，避免连接池耗尽）
_session = requests.Session()

# 推荐这样写，更稳健
_session.proxies = {"http": PROXY, "https": PROXY}


# =============================================================================
# 核心下载逻辑
# =============================================================================


def _extract_download_url(
    file_page_url: str,
    stop: Event,
    notify: Callable[[str, str, int, int | None, float, float], None],
) -> str | None:
    """
    从 upload.ee 文件页提取真实下载链接。

    参数:
        file_page_url: 文件页 URL
        stop:          取消事件（每次重试前检查）
        notify:        进度回调 (status, filename, dl, total, speed, %)

    返回:
        真实直链，失败返回 None
    """
    for attempt in range(1, MAX_RETRIES + 1):
        if stop.is_set():
            return None

        try:
            with _session.get(
                file_page_url,
                timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
                stream=True,  # 流式获取，避免一次加载到内存
            ) as resp:
                resp.raise_for_status()
                html = resp.text

            soup = BeautifulSoup(html, "html.parser")
            download_tag = soup.select_one("a[id='d_l']")
            if not download_tag:
                if attempt < MAX_RETRIES:
                    if stop.wait(RETRY_BACKOFF_BASE**attempt):
                        return None
                continue

            download_url = download_tag.get("href")
            if not download_url:
                if attempt < MAX_RETRIES:
                    if stop.wait(RETRY_BACKOFF_BASE**attempt):
                        return None
                continue

            return str(download_url)

        except requests.RequestException:
            if attempt < MAX_RETRIES:
                if stop.wait(RETRY_BACKOFF_BASE**attempt):
                    return None
        except Exception:
            if attempt < MAX_RETRIES:
                if stop.wait(RETRY_BACKOFF_BASE**attempt):
                    return None

    return None


def _get_remote_file_info(
    url: str,
    stop: Event,
) -> dict:
    """
    发送 HEAD 请求获取远程文件信息（支持 stop 取消）。

    返回:
        {"size": int|None, "accepts_ranges": bool, "filename": str|None}
    """
    info: dict = {"size": None, "accepts_ranges": False, "filename": None}

    for attempt in range(1, MAX_RETRIES + 1):
        if stop.is_set():
            return info

        try:
            resp = _session.head(
                url,
                timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
                allow_redirects=True,
            )
            # HEAD 返回 405 时降级为 GET+stream
            if resp.status_code == 405:
                resp.close()
                resp = _session.get(
                    url,
                    stream=True,
                    timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
                )

            with resp:
                content_length = resp.headers.get("Content-Length")
                if content_length:
                    info["size"] = int(content_length)

                accept_ranges = resp.headers.get("Accept-Ranges", "")
                info["accepts_ranges"] = "bytes" in accept_ranges.lower()

                disposition = resp.headers.get("Content-Disposition", "")
                if "filename=" in disposition:
                    m = re.search(
                        r'filename[*]?=(?:UTF-8\'\')?(?:"([^"]+)"|([^;]+))',
                        disposition,
                    )
                    if m:
                        info["filename"] = (m.group(1) or m.group(2)).strip()

            return info

        except requests.RequestException:
            if attempt < MAX_RETRIES:
                if stop.wait(RETRY_BACKOFF_BASE**attempt):
                    return info
        except Exception:
            if attempt < MAX_RETRIES:
                if stop.wait(RETRY_BACKOFF_BASE**attempt):
                    return info

    return info


def _download_with_resume(
    download_url: str,
    filepath: str,
    remote_size: int | None,
    *,
    chunk_size: int = CHUNK_SIZE,
    max_retries: int = MAX_RETRIES,
    progress_callback: Callable[[dict], None] | None = None,
    stop_event: Event | None = None,
) -> bool:
    """
    分块下载文件，支持断点续传和自动重试。

    参数:
        download_url:      直链
        filepath:          最终保存路径
        remote_size:       远程文件总大小（None=未知）
        chunk_size:        块大小
        max_retries:       最大重试
        progress_callback: 进度回调
        stop_event:        取消事件

    返回:
        True 成功, False 失败
    """
    tmp_file = f"{filepath}.part"
    stop = stop_event or Event()

    def _notify(
        status: str, fn: str, dl: int, total: int | None, speed: float, percent: float
    ) -> None:
        if progress_callback:
            progress_callback(
                {
                    "status": status,
                    "filename": fn,
                    "downloaded": dl,
                    "total": total,
                    "speed": speed,
                    "percent": percent,
                }
            )

    for attempt in range(1, max_retries + 1):
        if stop.is_set():
            return False

        try:
            # 检查断点
            part_size = 0
            headers: dict[str, str] = {}
            if os_path.exists(tmp_file):
                part_size = os_path.getsize(tmp_file)
                if remote_size and part_size >= remote_size:
                    rename(tmp_file, filepath)
                    return True
                if part_size > 0:
                    headers["Range"] = f"bytes={part_size}-"

            # 发起请求
            resp = _session.get(
                download_url,
                headers=headers,
                stream=True,
                timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
            )

            # 服务器不支持续传时回退
            if part_size > 0 and resp.status_code not in (206, 200):
                resp.close()
                part_size = 0
                with open(tmp_file, "wb"):
                    pass
                resp = _session.get(
                    download_url,
                    stream=True,
                    timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
                )

            if resp.status_code not in (200, 206):
                resp.close()
                if attempt < max_retries:
                    if stop.wait(RETRY_BACKOFF_BASE**attempt):
                        return False
                continue

            # 确定总大小
            total_size = remote_size
            if not total_size:
                if "Content-Length" in resp.headers:
                    total_size = int(resp.headers["Content-Length"]) + part_size
                elif "Content-Range" in resp.headers:
                    total_size = int(resp.headers["Content-Range"].split("/")[-1])

            # 分块写入
            start_time = time.time()
            downloaded = part_size
            last_report = start_time
            filename = os_path.basename(filepath)

            mode = "ab" if part_size else "wb"
            with resp:
                with open(tmp_file, mode) as f:
                    for chunk in resp.iter_content(chunk_size=chunk_size):
                        if stop.is_set():
                            return False
                        if not chunk:
                            continue
                        f.write(chunk)
                        downloaded += len(chunk)

                        now = time.time()
                        if now - last_report >= 0.5 or downloaded == total_size:
                            elap = now - start_time
                            spd = (downloaded - part_size) / elap if elap > 0 else 0
                            pct = (downloaded / total_size * 100) if total_size else 0
                            _notify(
                                "downloading",
                                filename,
                                downloaded,
                                total_size,
                                spd,
                                pct,
                            )
                            last_report = now

            # 校验
            if total_size and downloaded != total_size:
                if attempt < max_retries:
                    if stop.wait(RETRY_BACKOFF_BASE**attempt):
                        return False
                continue

            # 完成：临时文件 → 正式文件
            if os_path.exists(filepath):
                os_path.remove(filepath)
            rename(tmp_file, filepath)
            return True

        except requests.RequestException:
            if attempt < max_retries:
                if stop.wait(RETRY_BACKOFF_BASE**attempt):
                    return False
        except KeyboardInterrupt:
            return False
        except Exception:
            if attempt < max_retries:
                if stop.wait(RETRY_BACKOFF_BASE**attempt):
                    return False

    return False


# =============================================================================
# 公开 API
# =============================================================================


def downloader(
    url: str,
    output_dir: str | None = None,
    *,
    chunk_size: int | None = None,
    max_retries: int | None = None,
    progress_callback: Callable[[dict], None] | None = None,
    stop_event: Event | None = None,
) -> str | None:
    """
    从 upload.ee 文件页下载文件。

    参数:
        url:               upload.ee 文件页 URL
        output_dir:        保存目录（默认当前目录）
        chunk_size:        分块大小（默认 2MB）
        max_retries:       最大重试次数（默认 5）
        progress_callback: 进度回调 → {status, filename, downloaded, total, speed, percent}
        stop_event:        取消事件

    返回:
        成功返回文件路径，失败返回 None
    """
    output_dir = output_dir or getcwd()
    stop = stop_event or Event()
    _chunk = chunk_size or CHUNK_SIZE
    _retries = max_retries or MAX_RETRIES

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

    _notify("connecting", "Parsing download page...", 0, None, 0, 0)

    # Step 1: 提取直链
    download_url = _extract_download_url(url, stop, _notify)
    if not download_url:
        _notify("failed", "Failed to extract download link", 0, None, 0, 0)
        _log_error(f"upload.ee: failed to extract download link from {url}")
        return None

    # Step 2: 探测远程文件信息
    _notify("connecting", "Fetching file info...", 0, None, 0, 0)
    file_info = _get_remote_file_info(download_url, stop)

    if stop.is_set():
        return None

    remote_size = file_info["size"]

    # Step 3: 确定文件名和保存路径
    filename = file_info["filename"]
    if not filename:
        filename = download_url.rstrip("/").split("/")[-1].split("?")[0]
    if not filename:
        filename = "downloaded_file"

    filepath = os_path.join(output_dir, filename)

    # 已存在则跳过
    if os_path.exists(filepath) and os_path.getsize(filepath) > 0:
        size = os_path.getsize(filepath)
        _notify("completed", filename, size, size, 0, 100)
        return filepath

    # Step 4: 分块下载
    _notify("downloading", filename, 0, remote_size, 0, 0)
    success = _download_with_resume(
        download_url,
        filepath,
        remote_size,
        chunk_size=_chunk,
        max_retries=_retries,
        progress_callback=progress_callback,
        stop_event=stop,
    )

    if success:
        _notify(
            "completed",
            filename,
            os_path.getsize(filepath),
            os_path.getsize(filepath),
            0,
            100,
        )
    else:
        _notify("failed", filename, 0, remote_size, 0, 0)
        _log_error(f"upload.ee: download failed after retries: {download_url} → {filename}")

    return filepath if success else None
