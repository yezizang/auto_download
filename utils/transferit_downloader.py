#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Transfer.it 下载器模块
======================
使用 transferit-py 库下载文件。

URL 格式: https://transfer.it/t/<12-char-handle>

公开 API:
  download(url, output_dir, *, progress_callback, stop_event) -> str | None
"""

import os
import time
from pathlib import Path
from threading import Event
from typing import Callable

from transferit import Transferit, TransferNode

# =============================================================================
# 常量
# =============================================================================

PROXY: str = "socks5h://127.0.0.1:7891"

# httpx 走 socksio，scheme 用 socks5://
_httpx_proxy = PROXY.replace("socks5h://", "socks5://")
os.environ.setdefault("ALL_PROXY", _httpx_proxy)
os.environ.setdefault("HTTP_PROXY", _httpx_proxy)
os.environ.setdefault("HTTPS_PROXY", _httpx_proxy)


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
    从 transfer.it 下载文件。

    参数:
        url:               transfer.it 链接
        output_dir:        保存目录
        progress_callback: 进度回调 → {status, filename, downloaded, total, speed, percent}
        stop_event:        取消事件

    返回:
        成功返回首个文件路径，失败返回 None
    """
    stop = stop_event or Event()

    # ---- 进度报告 ----
    class State:
        total_bytes: int = 0
        agg_done: int = 0              # 所有文件已下载字节合计
        per_file: dict[str, int] = {}  # handle → downloaded
        current_name: str = ""
        last_ts: float = 0.0
        last_bytes: int = 0

    state = State()

    def _notify(status: str, filename: str, downloaded: int,
                total: int | None, speed: float, percent: float) -> None:
        if progress_callback:
            progress_callback({
                "status": status,
                "filename": filename,
                "downloaded": downloaded,
                "total": total,
                "speed": speed,
                "percent": percent,
            })

    def _on_start(files: list[TransferNode], total_bytes: int) -> None:
        state.total_bytes = total_bytes
        name = (files[0].name or files[0].handle) if files else ""
        state.current_name = name
        state.last_ts = time.perf_counter()
        _notify("downloading", name, 0, total_bytes, 0, 0)

    def _on_file_progress(node: TransferNode, downloaded: int, total: int) -> None:
        if stop.is_set():
            return

        key = node.handle
        state.per_file[key] = downloaded
        agg = sum(state.per_file.values())

        now = time.perf_counter()
        elap = now - state.last_ts
        if elap < 0.3 and agg < state.total_bytes:
            return

        speed = (agg - state.last_bytes) / elap if elap > 0 else 0
        pct = (agg / state.total_bytes * 100) if state.total_bytes else 0
        state.last_ts = now
        state.last_bytes = agg

        name = node.name or node.handle
        state.current_name = name
        _notify("downloading", name, agg, state.total_bytes, speed, pct)

    def _on_file_done(node: TransferNode, out_path: Path) -> None:
        key = node.handle
        state.per_file[key] = node.size or 0

    # ---- 下载 ----
    try:
        with Transferit() as tx:
            result = tx.download(
                url,
                output_dir,
                on_start=_on_start,
                on_file_progress=_on_file_progress,
                on_file_done=_on_file_done,
            )

        if result.paths:
            _notify("completed", state.current_name, state.total_bytes,
                     state.total_bytes, 0, 100)
            return result.paths[0]
        return None

    except Exception as e:
        if not stop.is_set():
            _notify("failed", str(e)[:120], 0, None, 0, 0)
        return None
