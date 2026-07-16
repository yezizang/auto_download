#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""统一错误日志，写入指定文件的尾部，每条带时间戳和完整堆栈。"""

import os
import traceback
from datetime import datetime, timezone

_LOG_PATH: str | None = None


def set_log_path(path: str) -> None:
    global _LOG_PATH
    _LOG_PATH = path


def error(msg: str, exc: BaseException | None = None) -> None:
    """记录一条错误，含时间戳、消息、完整 traceback。"""
    global _LOG_PATH
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    lines = [f"[{ts}] {msg}"]
    if exc is not None:
        lines.append("".join(traceback.format_exception(type(exc), exc, exc.__traceback__)))
    text = "\n".join(lines) + "\n\n"

    path = _LOG_PATH or os.path.join(os.getcwd(), "downloads", "errors.log")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(text)
