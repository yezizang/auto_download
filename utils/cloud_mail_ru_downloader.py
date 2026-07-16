#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cloud Mail.ru 下载器。

公开 API:
  resolve(url) -> list[dict]           # 解析文件夹 → [{url, filename}, ...]
  download(url, output_dir, ...)       # 下载单个文件（cloud.mail.ru-file 任务用）
  download_folder(url, output_dir, ...) # 下载整个文件夹（独立使用，汇总进度）
"""

import json
import os
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Event, Lock
from typing import Callable

import requests

from utils.logger import error as _log_error

CHUNK_SIZE = 2 * 1024 * 1024  # 2 MB
MAX_RETRIES = 3
TIMEOUT = (15, 120)
PROXY = "socks5h://127.0.0.1:7891"

# 并行下载线程数（仅 download_folder 使用）
DL_WORKERS = int(os.environ.get("AUTO_DL_WORKERS", "5"))

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
        _log_error(f"Node.js cloudSettings extraction failed: {result.stderr[:500]}")
        raise RuntimeError(f"Node.js failed: {result.stderr}")
    return json.loads(result.stdout.strip())


# =============================================================================
# 公开 API 1: 解析文件夹 → 文件直链列表
# =============================================================================

def resolve(url: str) -> list[dict]:
    """
    解析 cloud.mail.ru 文件夹页面，返回每个文件的下载直链。

    返回: [{"url": "https://...", "filename": "xxx.zip"}, ...]
    """
    session = requests.Session()
    session.headers.update({"User-Agent": _UA})
    session.proxies = {"http": PROXY, "https": PROXY}

    # Step 1: 抓取页面
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, timeout=TIMEOUT)
            resp.raise_for_status()
            break
        except requests.RequestException as e:
            if attempt == MAX_RETRIES:
                _log_error(f"cloud.mail.ru resolve: failed to fetch page {url}", exc=e)
                raise RuntimeError(f"Failed to fetch page: {e}")
            time.sleep(2 ** attempt)

    data = _extract_cloud_settings(resp.text)
    file_list = data.get("params", {}).get("serverSideFolders", {}).get("list", [])
    if not file_list:
        return []

    # Step 2: 逐个请求 zip 打包链接
    zip_api = "https://cloud.mail.ru/api/v3/zip/weblink"
    results: list[dict] = []

    for item in file_list:
        name = item.get("name", "download")
        weblink = item.get("weblink", "")
        if not weblink:
            continue

        post_body = {"x-email": "anonym", "weblink_list": [weblink], "name": name}
        safe_name = f"{name}.zip" if not name.endswith(".zip") else name
        safe_name = safe_name.replace("/", "_").replace("\\", "_")

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                api_resp = session.post(zip_api, json=post_body, timeout=TIMEOUT)
                api_resp.raise_for_status()
                break
            except requests.RequestException as e:
                if attempt == MAX_RETRIES:
                    _log_error(f"cloud.mail.ru zip API failed for: {name}", exc=e)
                    continue
                time.sleep(2 ** attempt)
        else:
            continue

        dl_key = api_resp.json().get("key")
        if not dl_key:
            continue

        download_url = (
            dl_key
            if str(dl_key).startswith("http")
            else f"https://cloud.mail.ru{dl_key}"
        )

        results.append({"url": download_url, "filename": safe_name})

    return results


# =============================================================================
# 公开 API 2: 下载单个文件（供 app.py 调度，每个文件一个 TaskInfo）
# =============================================================================

def download(
    url: str,
    output_dir: str,
    *,
    progress_callback: Callable[[dict], None] | None = None,
    stop_event: Event | None = None,
) -> str | None:
    """
    下载单个 cloud.mail.ru zip 文件（含断点续传）。

    返回 filepath 或 None。
    """
    stop = stop_event or Event()

    def _notify(status, filename, downloaded=0, total=None, speed=0, percent=0):
        if progress_callback:
            progress_callback(dict(
                status=status, filename=filename, downloaded=downloaded,
                total=total, speed=speed, percent=percent))

    # 从 URL 推断文件名
    safe_name = url.rstrip("/").rsplit("/", 1)[-1] or "download"
    if "?" in safe_name:
        safe_name = safe_name.split("?")[0]
    if not safe_name.endswith(".zip"):
        safe_name += ".zip"

    filepath = os.path.join(output_dir, safe_name)
    tmp = f"{filepath}.part"

    if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
        size = os.path.getsize(filepath)
        _notify("completed", safe_name, size, size, percent=100)
        return filepath

    _notify("connecting", safe_name)

    session = requests.Session()
    session.headers.update({"User-Agent": _UA})
    session.proxies = {"http": PROXY, "https": PROXY}

    for attempt in range(1, MAX_RETRIES + 1):
        if stop.is_set():
            return None

        part_size = os.path.getsize(tmp) if os.path.exists(tmp) else 0
        headers = {"Range": f"bytes={part_size}-"} if part_size > 0 else {}

        try:
            dl_resp = session.get(url, headers=headers, stream=True, timeout=TIMEOUT)
        except requests.RequestException as e:
            if attempt == MAX_RETRIES:
                _notify("failed", f"{safe_name}: {e}")
            else:
                time.sleep(2 ** attempt)
            continue

        if part_size > 0 and dl_resp.status_code not in (206, 200):
            part_size = 0
            open(tmp, "wb").close()
            try:
                dl_resp = session.get(url, stream=True, timeout=TIMEOUT)
            except requests.RequestException as e:
                if attempt == MAX_RETRIES:
                    _notify("failed", f"{safe_name}: {e}")
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
                        _notify("downloading", safe_name, downloaded,
                                remote_size, spd, pct)
                        last_rpt = now

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
            return filepath

        except requests.RequestException as e:
            if attempt == MAX_RETRIES:
                _notify("failed", f"{safe_name}: {e}")
            else:
                time.sleep(2 ** attempt)

    if os.path.exists(tmp):
        os.remove(tmp)
    _log_error(f"cloud.mail.ru download failed after {MAX_RETRIES} retries: {url} → {safe_name}")
    _notify("failed", f"{safe_name}: max retries exceeded")
    return None


# =============================================================================
# 公开 API 3: 下载整个文件夹（独立使用，汇总进度）
# =============================================================================

class _AggregateProgress:
    """多线程共享的汇总进度。"""

    def __init__(self, total_files: int, notify: Callable):
        self._lock = Lock()
        self._total = total_files
        self._done = 0
        self._bytes = 0
        self._start = time.perf_counter()
        self._notify = notify

    def add_bytes(self, n: int) -> None:
        with self._lock:
            self._bytes += n

    def file_completed(self) -> None:
        with self._lock:
            self._done += 1
            self._emit("completed" if self._done >= self._total else "downloading")

    def file_failed(self, name: str) -> None:
        with self._lock:
            self._notify("failed", name, 0, None, 0, 0)

    def report_chunk(self) -> None:
        with self._lock:
            self._emit("downloading")

    def _emit(self, status: str) -> None:
        label = f"cloud.mail.ru ({self._done}/{self._total})"
        elap = time.perf_counter() - self._start
        spd = self._bytes / elap if elap > 0 else 0
        self._notify(status, label, self._bytes, None, spd, 0)


def _download_one(task: dict, output_dir: str, agg: _AggregateProgress,
                  stop: Event) -> str | None:
    """内部：下载单个文件（汇总进度版）。"""
    safe_name = task["safe_name"]
    download_url = task["download_url"]
    filepath = os.path.join(output_dir, safe_name)
    tmp = f"{filepath}.part"

    if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
        agg.file_completed()
        return filepath

    session = requests.Session()
    session.headers.update({"User-Agent": _UA})
    session.proxies = {"http": PROXY, "https": PROXY}

    for attempt in range(1, MAX_RETRIES + 1):
        if stop.is_set():
            return None

        part_size = os.path.getsize(tmp) if os.path.exists(tmp) else 0
        headers = {"Range": f"bytes={part_size}-"} if part_size > 0 else {}

        try:
            dl_resp = session.get(download_url, headers=headers,
                                  stream=True, timeout=TIMEOUT)
        except requests.RequestException as e:
            if attempt == MAX_RETRIES:
                agg.file_failed(f"{safe_name}: {e}")
            else:
                time.sleep(2 ** attempt)
            continue

        if part_size > 0 and dl_resp.status_code not in (206, 200):
            part_size = 0
            open(tmp, "wb").close()
            try:
                dl_resp = session.get(download_url, stream=True, timeout=TIMEOUT)
            except requests.RequestException as e:
                if attempt == MAX_RETRIES:
                    agg.file_failed(f"{safe_name}: {e}")
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
        downloaded = part_size
        last_rpt = 0.0

        try:
            with open(tmp, mode) as f:
                for chunk in dl_resp.iter_content(chunk_size=CHUNK_SIZE):
                    if stop.is_set():
                        return None
                    if not chunk:
                        continue
                    f.write(chunk)
                    downloaded += len(chunk)
                    agg.add_bytes(len(chunk))

                    now = time.perf_counter()
                    if now - last_rpt >= 0.5 or downloaded == remote_size:
                        agg.report_chunk()
                        last_rpt = now

            if remote_size and os.path.getsize(tmp) != remote_size:
                if attempt < MAX_RETRIES:
                    time.sleep(2 ** attempt)
                continue

            if os.path.exists(filepath):
                os.remove(filepath)
            os.rename(tmp, filepath)

            agg.file_completed()
            return filepath

        except requests.RequestException as e:
            if attempt == MAX_RETRIES:
                agg.file_failed(f"{safe_name}: {e}")
            else:
                time.sleep(2 ** attempt)

    if os.path.exists(tmp):
        os.remove(tmp)
    agg.file_failed(f"{safe_name}: max retries exceeded")
    return None


def download_folder(
    url: str,
    output_dir: str,
    *,
    progress_callback: Callable[[dict], None] | None = None,
    stop_event: Event | None = None,
) -> list[str]:
    """下载整个 cloud.mail.ru 文件夹（独立使用，汇总进度报告）。"""
    stop = stop_event or Event()

    def _notify(status, filename, downloaded=0, total=None, speed=0, percent=0):
        if progress_callback:
            progress_callback(dict(
                status=status, filename=filename, downloaded=downloaded,
                total=total, speed=speed, percent=percent))

    # Step 1 + 2: 解析获取所有下载链接
    _notify("connecting", "cloud.mail.ru", 0, None, 0, 0)
    try:
        resolved = resolve(url)
    except Exception as e:
        _notify("failed", str(e)[:120])
        return []

    if not resolved:
        _notify("failed", "No files found")
        return []

    tasks = [{"safe_name": r["filename"], "download_url": r["url"]} for r in resolved]

    # Step 3: 并行下载
    agg = _AggregateProgress(len(tasks), _notify)
    _notify("downloading", f"cloud.mail.ru (0/{len(tasks)})", 0, None, 0, 0)

    results: list[str] = []
    with ThreadPoolExecutor(max_workers=DL_WORKERS, thread_name_prefix="clouddl") as pool:
        futures = {
            pool.submit(_download_one, t, output_dir, agg, stop): t
            for t in tasks
        }
        for future in as_completed(futures):
            if stop.is_set():
                for f in futures:
                    f.cancel()
                break
            path = future.result()
            if path:
                results.append(path)

    return results
