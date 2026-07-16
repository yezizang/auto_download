#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pixeldrain 下载器模块
=====================
使用官方 Pixeldrain 库获取文件信息，然后流式下载。

公开 API:
  download(url, output_dir, *, progress_callback, stop_event) -> str | None
"""

import os
import re
import time
from threading import Event
from typing import Callable

import requests
import pixeldrain

from utils.logger import error as _log_error


# =============================================================================
# 常量
# =============================================================================

CHUNK_SIZE: int = 2 * 1024 * 1024  # 2 MB
MAX_RETRIES: int = 5
CONNECT_TIMEOUT: float = 15.0
READ_TIMEOUT: float = 120.0
PROXY: str = "socks5h://127.0.0.1:7891"

# Pixeldrain 链接中提取 file_id 的正则
_URL_PATTERN = re.compile(r"pixeldrain\.com/(?:u|l|api/file)/([a-zA-Z0-9]+)")


# =============================================================================
# PixeldrainDownloader
# =============================================================================

class PixeldrainDownloader:
    """使用 pixeldrain 库获取信息 + 流式下载。"""

    def __init__(
        self,
        url: str,
        output_dir: str,
        progress_callback: Callable[[dict], None] | None = None,
        stop_event: Event | None = None,
    ):
        self._url = url
        self._output_dir = output_dir
        self._progress_callback = progress_callback
        self._stop_event = stop_event or Event()
        self._session = requests.Session()
        self._session.proxies = {"http": PROXY, "https": PROXY}
        self._file_id: str | None = None

    def _extract_id(self) -> str | None:
        """从 URL 提取 Pixeldrain 文件 ID。"""
        m = _URL_PATTERN.search(self._url)
        return m.group(1) if m else None

    def _notify(self, status: str, filename: str, downloaded: int,
                total: int | None, speed: float, percent: float) -> None:
        if self._progress_callback:
            self._progress_callback({
                "status": status,
                "filename": filename,
                "downloaded": downloaded,
                "total": total,
                "speed": speed,
                "percent": percent,
            })

    def run(self) -> str | None:
        """
        执行下载。返回保存路径，失败返回 None。
        """
        # 1. 提取 file_id
        self._file_id = self._extract_id()
        if not self._file_id:
            self._notify("failed", "Invalid URL", 0, None, 0, 0)
            _log_error(f"pixeldrain: invalid URL, could not extract file_id: {self._url}")
            return None

        # 2. 获取文件信息（用 self._session 走代理，不用 pixeldrain.info）
        self._notify("connecting", "Fetching file info...", 0, None, 0, 0)
        try:
            resp = self._session.get(
                f"https://pixeldrain.com/api/file/{self._file_id}/info",
                timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
            )
            resp.raise_for_status()
            file_info = resp.json()
        except Exception:
            self._notify("failed", "Failed to get file info", 0, None, 0, 0)
            _log_error(f"pixeldrain: failed to get file info for {file_id}")
            return None

        filename = file_info.get("name") or self._file_id
        remote_size = file_info.get("size")

        # 3. 确定保存路径
        filepath = os.path.join(self._output_dir, filename)

        # 检查已存在
        if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
            size = os.path.getsize(filepath)
            self._notify("completed", filename, size, size, 0, 100)
            return filepath

        # 4. 获取下载直链（来自 pixeldrain 库）
        download_url = pixeldrain.file(self._file_id)

        # 5. 流式下载（分块 + 断点续传）
        self._notify("downloading", filename, 0, remote_size, 0, 0)
        ok = self._do_download(download_url, filepath, remote_size, filename)
        return filepath if ok else None

    def _do_download(
        self, url: str, filepath: str, remote_size: int | None, filename: str,
    ) -> bool:
        """分块流式下载，支持断点续传。"""
        tmp = f"{filepath}.part"

        for attempt in range(1, MAX_RETRIES + 1):
            if self._stop_event.is_set():
                return False

            headers = {}
            part_size = 0
            if os.path.exists(tmp):
                part_size = os.path.getsize(tmp)
                if remote_size and part_size >= remote_size:
                    self._finish(tmp, filepath)
                    return True
                if part_size > 0:
                    headers["Range"] = f"bytes={part_size}-"

            try:
                resp = self._session.get(
                    url, headers=headers, stream=True,
                    timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
                )

                # 处理服务器不支持断点续传的情况
                if part_size > 0 and resp.status_code not in (206, 200):
                    part_size = 0
                    open(tmp, "wb").close()
                    resp = self._session.get(
                        url, stream=True,
                        timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
                    )

                if resp.status_code not in (200, 206):
                    if attempt < MAX_RETRIES:
                        time.sleep(2 ** attempt)
                    continue

                # 确定总大小
                total = remote_size
                if not total:
                    if "Content-Length" in resp.headers:
                        total = int(resp.headers["Content-Length"]) + part_size
                    elif "Content-Range" in resp.headers:
                        total = int(resp.headers["Content-Range"].split("/")[-1])

                # 分块写入
                t0 = time.perf_counter()
                downloaded = part_size
                last_rpt = 0.0
                mode = "ab" if part_size else "wb"

                with open(tmp, mode) as f:
                    for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                        if self._stop_event.is_set():
                            return False
                        if not chunk:
                            continue
                        f.write(chunk)
                        downloaded += len(chunk)

                        now = time.perf_counter()
                        if now - last_rpt >= 0.5 or downloaded == total:
                            elap = now - t0
                            spd = (downloaded - part_size) / elap if elap > 0 else 0
                            pct = (downloaded / total * 100) if total else 0
                            self._notify("downloading", filename, downloaded, total, spd, pct)
                            last_rpt = now

                # 校验
                if total and os.path.getsize(tmp) != total:
                    if attempt < MAX_RETRIES:
                        time.sleep(2 ** attempt)
                    continue

                self._finish(tmp, filepath)

                elap = time.perf_counter() - t0
                avg = (downloaded - part_size) / elap if elap > 0 else 0
                self._notify("completed", filename, downloaded, total or downloaded, avg, 100)
                return True

            except requests.RequestException:
                if attempt < MAX_RETRIES:
                    time.sleep(2 ** attempt)
            except Exception:
                if attempt < MAX_RETRIES:
                    time.sleep(2 ** attempt)

        self._notify("failed", filename, 0, remote_size, 0, 0)
        _log_error(f"pixeldrain: download failed after retries: {url} → {filename}")
        return False

    @staticmethod
    def _finish(tmp: str, filepath: str) -> None:
        """将临时文件重命名为最终文件。"""
        if os.path.exists(filepath):
            os.remove(filepath)
        os.rename(tmp, filepath)


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
    从 Pixeldrain 下载文件。

    参数:
        url:               Pixeldrain 链接
        output_dir:        保存目录
        progress_callback: 进度回调 → {status, filename, downloaded, total, speed, percent}
        stop_event:        取消事件

    返回:
        成功返回文件路径，失败返回 None
    """
    dl = PixeldrainDownloader(url, output_dir, progress_callback, stop_event)
    return dl.run()
