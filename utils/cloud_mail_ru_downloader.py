#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cloud Mail.ru 下载器 — 从文件夹页面提取文件列表，逐个打包为 zip 下载。

公开 API:
  download(url, output_dir, *, progress_callback, stop_event) -> list[str]
"""

import json
import os
import subprocess
import time
from threading import Event
from typing import Callable

import requests

CHUNK_SIZE = 2 * 1024 * 1024  # 2 MB
MAX_RETRIES = 3
TIMEOUT = (15, 120)
PROXY = "socks5h://127.0.0.1:7891"

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36 Edg/150.0.0.0"
)

# ---- Node.js 脚本：从 HTML 中 eval 出 window.cloudSettings 并导出 JSON ----
_NODE_SCRIPT = r"""
let html = '';
process.stdin.setEncoding('utf8');
process.stdin.on('data', chunk => html += chunk);
process.stdin.on('end', () => {
    const idx = html.indexOf('window.cloudSettings');
    if (idx === -1) { console.log('{}'); process.exit(0); }
    const braceStart = html.indexOf('{', idx);
    let depth = 0, inString = false, esc = false, end = braceStart;
    for (let i = braceStart; i < html.length; i++) {
        const ch = html[i];
        if (esc) { esc = false; continue; }
        if (ch === '\\') { esc = true; continue; }
        if (ch === '"' || ch === "'") {
            if (!inString) inString = ch;
            else if (ch === inString) inString = false;
            continue;
        }
        if (inString) continue;
        if (ch === '{') depth++;
        else if (ch === '}') { depth--; if (depth === 0) { end = i + 1; break; } }
    }
    const data = eval('(' + html.substring(braceStart, end) + ')');
    console.log(JSON.stringify(data));
});
"""


def _extract_cloud_settings(html: str) -> dict:
    """用 Node.js eval 解析页面中的 cloudSettings JS 对象。"""
    result = subprocess.run(
        ["node", "-e", _NODE_SCRIPT],
        input=html,
        capture_output=True,
        text=True,
        timeout=30,
        encoding="utf-8",
        errors="replace",
    )
    if result.returncode != 0:
        raise RuntimeError(f"Node.js failed: {result.stderr}")
    return json.loads(result.stdout.strip())


# ---- 公开 API ----


def download(
    url: str,
    output_dir: str,
    *,
    progress_callback: Callable[[dict], None] | None = None,
    stop_event: Event | None = None,
) -> list[str]:
    stop = stop_event or Event()

    def _notify(status, filename, downloaded=0, total=None, speed=0, percent=0):
        if progress_callback:
            progress_callback(
                dict(
                    status=status,
                    filename=filename,
                    downloaded=downloaded,
                    total=total,
                    speed=speed,
                    percent=percent,
                )
            )

    session = requests.Session()
    session.headers.update({"User-Agent": _UA})
    session.proxies = {"http": PROXY, "https": PROXY}

    # ---- Step 1: 抓取页面，提取文件列表 ----
    _notify("connecting", "", 0, None, 0, 0)
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, timeout=TIMEOUT)
            resp.raise_for_status()
            break
        except requests.RequestException as e:
            if attempt == MAX_RETRIES:
                _notify("failed", str(e)[:120])
                return []
            time.sleep(2 ** attempt)

    try:
        data = _extract_cloud_settings(resp.text)
    except Exception as e:
        _notify("failed", f"Parse error: {e}")
        return []

    file_list = data.get("params", {}).get("serverSideFolders", {}).get("list", [])
    if not file_list:
        _notify("failed", "No files found")
        return []

    # ---- Step 2: 逐个文件请求 zip 打包并下载 ----
    zip_api = "https://cloud.mail.ru/api/v3/zip/weblink"
    results: list[str] = []

    for item in file_list:
        if stop.is_set():
            break

        name = item.get("name", "download")
        weblink = item.get("weblink", "")
        if not weblink:
            continue

        post_body = {"x-email": "anonym", "weblink_list": [weblink], "name": name}

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                api_resp = session.post(zip_api, json=post_body, timeout=TIMEOUT)
                api_resp.raise_for_status()
                break
            except requests.RequestException as e:
                if attempt == MAX_RETRIES:
                    _notify("failed", f"{name}: API {e}")
                    continue
                time.sleep(2 ** attempt)
        else:
            continue

        dl_key = api_resp.json().get("key")
        if not dl_key:
            _notify("failed", f"{name}: no download key")
            continue

        # key 可能是完整 URL 或路径
        download_url = (
            dl_key
            if str(dl_key).startswith("http")
            else f"https://cloud.mail.ru{dl_key}"
        )

        # 安全文件名（cloud.mail.ru zip 打包输出为 zip）
        safe_name = name if name.endswith(".zip") else f"{name}.zip"
        # ponytail: 简单清理危险字符
        safe_name = safe_name.replace("/", "_").replace("\\", "_")
        filepath = os.path.join(output_dir, safe_name)

        # 跳过已存在且非空的文件
        if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
            size = os.path.getsize(filepath)
            _notify("completed", safe_name, size, size, percent=100)
            results.append(filepath)
            continue

        # ---- Step 3: 流式下载（含断点续传） ----
        _notify("downloading", safe_name)

        tmp = f"{filepath}.part"

        for attempt in range(1, MAX_RETRIES + 1):
            if stop.is_set():
                return results

            part_size = os.path.getsize(tmp) if os.path.exists(tmp) else 0
            headers = {"Range": f"bytes={part_size}-"} if part_size > 0 else {}

            try:
                dl_resp = session.get(download_url, headers=headers,
                                      stream=True, timeout=TIMEOUT)
            except requests.RequestException as e:
                if attempt == MAX_RETRIES:
                    _notify("failed", f"{name}: {e}")
                else:
                    time.sleep(2 ** attempt)
                continue

            # 服务器不支持断点续传，重头开始
            if part_size > 0 and dl_resp.status_code not in (206, 200):
                part_size = 0
                open(tmp, "wb").close()
                try:
                    dl_resp = session.get(download_url, stream=True, timeout=TIMEOUT)
                except requests.RequestException as e:
                    if attempt == MAX_RETRIES:
                        _notify("failed", f"{name}: {e}")
                    else:
                        time.sleep(2 ** attempt)
                    continue

            if dl_resp.status_code not in (200, 206):
                if attempt < MAX_RETRIES:
                    time.sleep(2 ** attempt)
                continue

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

            try:
                with open(tmp, mode) as f:
                    for chunk in dl_resp.iter_content(chunk_size=CHUNK_SIZE):
                        if stop.is_set():
                            return results
                        if not chunk:
                            continue
                        f.write(chunk)
                        downloaded += len(chunk)

                        now = time.perf_counter()
                        if now - last_rpt >= 0.5 or downloaded == remote_size:
                            elap = now - t0
                            spd = (downloaded - part_size) / elap if elap > 0 else 0
                            pct = (downloaded / remote_size * 100) if remote_size else 0
                            _notify("downloading", safe_name, downloaded,
                                    remote_size, spd, pct)
                            last_rpt = now

                # 校验文件大小
                if remote_size and os.path.getsize(tmp) != remote_size:
                    if attempt < MAX_RETRIES:
                        time.sleep(2 ** attempt)
                    continue

                if os.path.exists(filepath):
                    os.remove(filepath)
                os.rename(tmp, filepath)

                elap = time.perf_counter() - t0
                avg = (downloaded - part_size) / elap if elap > 0 else 0
                _notify("completed", safe_name, downloaded,
                        remote_size or downloaded, avg, 100)
                results.append(filepath)
                break

            except requests.RequestException as e:
                # ponytail: 保留 .part 用于断点续传
                if attempt == MAX_RETRIES:
                    _notify("failed", f"{name}: {e}")
                else:
                    time.sleep(2 ** attempt)
        else:
            # 所有重试都失败了
            if os.path.exists(tmp):
                os.remove(tmp)
            _notify("failed", f"{name}: max retries exceeded")

    return results
