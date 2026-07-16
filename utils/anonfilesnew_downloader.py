#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AnonFilesNew 下载器模块
=======================
破解 anonfilesnew.com 的 JS 混淆，提取真实下载直链并下载。

反爬机制:
  页面内嵌一段混淆 JS，动态计算出真实的 /content/v4?s=... 下载 URL。
  解码算法: atob → XOR(动态基值 + 字符位置 + 排序索引) → 拼接 → basePath

公开 API:
  download(url, output_dir, *, progress_callback, stop_event) -> str | None
"""

import base64
import os
import re
import time
from threading import Event
from typing import Callable

import requests

from utils.logger import error as _log_error

# =============================================================================
# 常量
# =============================================================================

CHUNK_SIZE: int = 2 * 1024 * 1024  # 2 MB
MAX_RETRIES: int = 5
CONNECT_TIMEOUT: float = 15.0
READ_TIMEOUT: float = 120.0
PROXY: str = "socks5h://127.0.0.1:7891"

# 匹配 anonfilesnew.com 的分享链接
_URL_PATTERN = re.compile(r"anonfilesnew\.com/(?:s/)?([A-Za-z0-9_-]+)")


# =============================================================================
# JS 混淆解码
# =============================================================================

def _decode_download_url(html: str) -> str:
    """从页面 HTML 中提取并解码出真实下载 URL。"""
    # 定位包含 u={ 和 o=[ 的 script 块
    idx = html.find("u={")
    if idx == -1:
        raise ValueError("u={ not found in page HTML")

    block_start = html.rfind("<script>", 0, idx)
    block_end = html.find("</script>", idx)
    script = html[block_start:block_end]

    # 1. 提取 XOR 基值: a.charCodeAt(t)^NNN+t+o[n][1]
    xor_match = re.search(r"charCodeAt\(t\)\^(\d+)\+t\+o\[n\]\[1\]", script)
    if not xor_match:
        raise ValueError("XOR base not found")
    xor_base = int(xor_match.group(1))

    # 2. 提取 u dict: {_key: "base64value", ...}
    u_match = re.search(r"u=\{([^}]+)\}", script)
    if not u_match:
        raise ValueError("u dict not found")
    u_pairs = re.findall(r'(_\w+):"([^"]*)"', u_match.group(1))
    u = dict(u_pairs)

    # 3. 提取 o array: [["_key", index], ...]  —— 用括号计数
    o_start = script.find("o=[")
    if o_start == -1:
        raise ValueError("o array not found")
    depth = 0
    o_end = o_start + 2
    for i in range(o_start + 2, len(script)):
        if script[i] == "[":
            depth += 1
        elif script[i] == "]":
            depth -= 1
            if depth == 0:
                o_end = i + 1
                break
    o_pairs = re.findall(r'\["(_\w+)",(\d+)\]', script[o_start + 2 : o_end])
    o = [[k, int(v)] for k, v in o_pairs]

    # 4. 解码: 排序 → atob → XOR(xor_base + t + idx)
    o.sort(key=lambda x: x[1])
    result = ""
    for key, idx in o:
        b64 = u.get(key, "")
        if not b64:
            continue
        padding = 4 - len(b64) % 4
        if padding != 4:
            b64 += "=" * padding
        decoded = base64.b64decode(b64)
        chars = []
        for t, byte in enumerate(decoded):
            chars.append(chr(byte ^ (xor_base + t + idx)))
        result += "".join(chars)

    return result


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
    从 anonfilesnew.com 下载文件。

    参数:
        url:               分享链接 (如 https://anonfilesnew.com/s/XjoK5HccKAU)
        output_dir:        保存目录
        progress_callback: 进度回调 → {status, filename, downloaded, total, speed, percent}
        stop_event:        取消事件

    返回:
        成功返回文件路径，失败返回 None
    """
    stop = stop_event or Event()
    session = requests.Session()
    session.proxies = {"http": PROXY, "https": PROXY}
    session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})

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

    # ---- Step 1: 抓页面，解码下载直链 ----
    _notify("connecting", "Fetching page...", 0, None, 0, 0)
    try:
        resp = session.get(url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
        resp.raise_for_status()
        download_url = _decode_download_url(resp.text)
    except Exception as e:
        _notify("failed", str(e)[:120], 0, None, 0, 0)
        _log_error(f"anonfilesnew: failed to fetch or decode page: {url}", exc=e)
        return None

    if stop.is_set():
        return None

    # ---- Step 2: HEAD 请求获取文件名和大小 ----
    try:
        head = session.head(download_url, allow_redirects=True,
                            timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
    except Exception:
        _notify("failed", "Failed to fetch file info", 0, None, 0, 0)
        _log_error(f"anonfilesnew: HEAD request failed: {download_url}")
        return None

    cd = head.headers.get("content-disposition", "")
    filename = None
    if cd:
        fname_match = re.search(r'filename[^;=\n]*=["\']?(.*?)["\']?(?:;|$)', cd, re.I)
        if fname_match:
            filename = fname_match.group(1).strip('"\' ')
    if not filename:
        # 从 URL 末尾或页面标题提取
        filename = url.rstrip("/").rsplit("/", 1)[-1] or "download"

    remote_size = None
    cl = head.headers.get("content-length")
    if cl:
        try:
            remote_size = int(cl)
        except ValueError:
            pass

    # ---- Step 3: 确定保存路径 ----
    filepath = os.path.join(output_dir, filename)

    # 跳过已完成
    if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
        size = os.path.getsize(filepath)
        _notify("completed", filename, size, size, 0, 100)
        return filepath

    # ---- Step 4: 流式下载 ----
    _notify("downloading", filename, 0, remote_size, 0, 0)
    tmp = f"{filepath}.part"

    for attempt in range(1, MAX_RETRIES + 1):
        if stop.is_set():
            return None

        part_size = 0
        headers = {}
        if os.path.exists(tmp):
            part_size = os.path.getsize(tmp)
            if remote_size and part_size >= remote_size:
                os.rename(tmp, filepath)
                _notify("completed", filename, remote_size, remote_size, 0, 100)
                return filepath
            if part_size > 0:
                headers["Range"] = f"bytes={part_size}-"

        try:
            resp2 = session.get(
                download_url,
                headers=headers,
                stream=True,
                timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
            )

            if part_size > 0 and resp2.status_code not in (206, 200):
                part_size = 0
                open(tmp, "wb").close()
                resp2 = session.get(
                    download_url, stream=True,
                    timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
                )

            if resp2.status_code not in (200, 206):
                if attempt < MAX_RETRIES:
                    time.sleep(2 ** attempt)
                continue

            # 确定总大小
            total = remote_size
            if not total:
                cl2 = resp2.headers.get("content-length")
                if cl2:
                    total = int(cl2) + part_size

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
                    if now - last_rpt >= 0.5 or downloaded == total:
                        elap = now - t0
                        spd = (downloaded - part_size) / elap if elap > 0 else 0
                        pct = (downloaded / total * 100) if total else 0
                        _notify("downloading", filename, downloaded, total, spd, pct)
                        last_rpt = now

            # 校验
            if total and os.path.getsize(tmp) != total:
                if attempt < MAX_RETRIES:
                    time.sleep(2 ** attempt)
                continue

            # 完成
            if os.path.exists(filepath):
                os.remove(filepath)
            os.rename(tmp, filepath)

            elap = time.perf_counter() - t0
            avg = (downloaded - part_size) / elap if elap > 0 else 0
            _notify("completed", filename, downloaded, total or downloaded, avg, 100)
            return filepath

        except requests.RequestException:
            if attempt < MAX_RETRIES:
                time.sleep(2 ** attempt)
        except Exception:
            if attempt < MAX_RETRIES:
                time.sleep(2 ** attempt)

    _notify("failed", filename, 0, remote_size, 0, 0)
    _log_error(f"anonfilesnew: download failed after retries: {url} → {filename}")
    return None
