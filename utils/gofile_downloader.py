#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GoFile 下载器模块（重构版）
==========================
从 GoFile (https://gofile.io) 网盘下载文件。

基于 gofile-dl (https://github.com/martadams89/gofile-dl) 的 API 逻辑重写，
适配 2026 年 7 月 GoFile 改版后的 API。

关键变更（相对于旧版）:
  - 账户创建时不发送 X-Website-Token
  - 使用完整 Chrome User-Agent（必须与 website token 计算的 UA 一致）
  - 内容 API 使用新的查询参数格式
  - 下载请求携带 Cookie: accountToken=...
  - 支持 token 时间窗口边界回退重试
  - 支持 rate-limit 退避重试
  - 可选 curl_cffi TLS 指纹模拟（通过 GOFILE_IMPERSONATE 环境变量）

公开 API:
  download(url, output_dir, *, password, progress_callback, stop_event) -> list[str] | None
"""

import os
import time
import shutil
import re
from hashlib import sha256
from itertools import count
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from typing import Callable

import requests
from requests.structures import CaseInsensitiveDict

from utils.logger import error as _log_error

# =============================================================================
# 常量
# =============================================================================

CHUNK_SIZE: int = 2 * 1024 * 1024  # 2 MB
MAX_RETRIES: int = 5
TIMEOUT: float = 30.0
CONTENT_TIMEOUT: float = 45.0
MAX_WORKERS: int = 5

# 代理配置（优先级：环境变量 GOFILE_PROXY > ALL_PROXY > HTTPS_PROXY > 默认代理）
PROXY: str = (
    os.environ.get("GOFILE_PROXY", "").strip()
    or os.environ.get("ALL_PROXY")
    or os.environ.get("HTTPS_PROXY")
    or "http://127.0.0.1:7891"
)

# GoFile API edge 封锁检测关键词
_RESET_HINTS: tuple[str, ...] = (
    "reset by peer",
    "connection aborted",
    "err_empty_response",
    "recv failure",
    "curl: (35)",
    "curl: (56)",
    "connection reset",
)

# GoFile 2026 API 要求的完整 Chrome User-Agent（必须与 website token 计算一致）
GOFILE_USER_AGENT: str = os.environ.get(
    "GOFILE_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
)
GOFILE_LANGUAGE: str = os.environ.get("GOFILE_LANGUAGE", "en-US")

# website token 的盐值，取自 gofile.io 前端 wt.obf.js
GOFILE_WT_SALT: str = os.environ.get("GOFILE_WT_SALT", "9844d94d963d30")
WT_WINDOW_SECONDS: int = 14400  # 4 小时轮换窗口

# 内容 API 查询参数（匹配 GoFile 前端 2026 版）
CONTENTS_QUERY_PARAMS: dict = {
    "contentFilter": "",
    "page": 1,
    "pageSize": 1000,
    "sortField": "createTime",
    "sortDirection": -1,
}

# curl_cffi 可选 TLS 指纹模拟
GOFILE_IMPERSONATE: str = os.environ.get("GOFILE_IMPERSONATE", "chrome").strip()
try:
    from curl_cffi import requests as _cffi_requests  # type: ignore

    _HAS_CFFI: bool = bool(GOFILE_IMPERSONATE) and GOFILE_IMPERSONATE.lower() != "off"
except ImportError:
    _cffi_requests = None
    _HAS_CFFI = False


# =============================================================================
# 辅助函数
# =============================================================================


def generate_website_token(account_token: str, window_offset: int = 0) -> str:
    """
    生成 GoFile API 需要的动态 X-Website-Token。

    算法来源: gofile.io 前端 wt.obf.js
        sha256(f"{user_agent}::{language}::{account_token}::{window}::{salt}")

    每 4 小时轮换一次 (window = floor(unix_time / 14400))。

    Args:
        account_token: 账户令牌（来自 POST /accounts）
        window_offset: 时间窗口偏移（0=当前窗口, -1=上一个窗口）
    """
    window = int(time.time() // WT_WINDOW_SECONDS) + window_offset
    raw = (
        f"{GOFILE_USER_AGENT}::{GOFILE_LANGUAGE}"
        f"::{account_token}::{window}::{GOFILE_WT_SALT}"
    )
    return sha256(raw.encode("utf-8")).hexdigest()


def _is_edge_block(exc: Exception) -> bool:
    """检测 GoFile API edge 是否重置了连接（说明 IP 被封锁）。"""
    text = f"{type(exc).__name__}: {exc}".lower()
    return any(hint in text for hint in _RESET_HINTS)


def _api_request(method: str, url: str, **kwargs) -> requests.Response:
    """
    统一 HTTP 请求，支持代理和 curl_cffi TLS 指纹模拟。

    当设置了 ALL_PROXY / HTTPS_PROXY 时通过代理访问；
    当安装了 curl_cffi 且 GOFILE_IMPERSONATE != "off" 时使用浏览器 TLS 指纹。
    """
    if PROXY:
        kwargs.setdefault("proxies", {"http": PROXY, "https": PROXY})
    if _HAS_CFFI and _cffi_requests is not None:
        return _cffi_requests.request(
            method, url, impersonate=GOFILE_IMPERSONATE, **kwargs
        )
    return requests.request(method, url, **kwargs)


# =============================================================================
# GoFileDownloader 类
# =============================================================================


class GoFileDownloader:
    """
    GoFile 下载器。

    用法:
        dl = GoFileDownloader("https://gofile.io/d/abc123", "/save/here")
        result = dl.run()  # 返回下载的文件路径列表
    """

    def __init__(
        self,
        url: str,
        output_dir: str,
        password: str | None = None,
        progress_callback: Callable[[dict], None] | None = None,
        stop_event: Event | None = None,
    ):
        self._url = url
        self._output_dir = output_dir
        self._password = password
        self._progress_callback = progress_callback
        self._stop_event = stop_event or Event()

        # 存储文件信息: { "0": {"path": ..., "filename": ..., "link": ...}, ... }
        self._files_info: dict[str, dict[str, str]] = {}

        # 账户令牌（由 _setup_account 获取）
        self._account_token: str = ""

        # HTTP 会话
        self._session = requests.Session()
        if PROXY:
            self._session.proxies = {"http": PROXY, "https": PROXY}
        self._session.headers.update(
            {
                "User-Agent": GOFILE_USER_AGENT,
                "Accept": "*/*",
                "Accept-Encoding": "gzip",
                "Connection": "keep-alive",
                "Origin": "https://gofile.io",
                "Referer": "https://gofile.io/",
            }
        )

        # 获取账户令牌
        self._setup_account()

    # -------------------------------------------------------------------------
    # 账户认证
    # -------------------------------------------------------------------------

    def _setup_account(self) -> None:
        """
        创建匿名 GoFile 账户以获取访问令牌。

        注意：账户创建 API 不需要 X-Website-Token（与内容 API 不同）。
        """
        for attempt in range(MAX_RETRIES):
            if self._stop_event.is_set():
                return
            try:
                resp = _api_request(
                    "POST",
                    "https://api.gofile.io/accounts",
                    headers={
                        "User-Agent": GOFILE_USER_AGENT,
                        "Origin": "https://gofile.io",
                    },
                    timeout=TIMEOUT,
                ).json()
                if resp.get("status") == "ok":
                    self._account_token = resp["data"]["token"]
                    self._session.headers.update(
                        {"Authorization": f"Bearer {self._account_token}"}
                    )
                    return
                _log_error(
                    f"gofile: account creation returned unexpected status: {resp}"
                )
            except requests.Timeout:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2)
                    continue
                _log_error("gofile: account creation timed out after all retries")
            except Exception as e:
                if _is_edge_block(e):
                    _log_error(
                        "gofile: GoFile API edge blocked the connection "
                        "(account creation). Try setting GOFILE_PROXY or "
                        "installing curl_cffi."
                    )
                    break
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2)
                    continue
                _log_error(f"gofile: account creation failed: {type(e).__name__}: {e}")

    # -------------------------------------------------------------------------
    # HTTP 请求
    # -------------------------------------------------------------------------

    def _content_headers(self, window_offset: int = 0) -> dict:
        """构建 /contents API 请求头。"""
        return {
            "Authorization": f"Bearer {self._account_token}",
            "X-Website-Token": generate_website_token(
                self._account_token, window_offset
            ),
            "X-BL": GOFILE_LANGUAGE,
            "User-Agent": GOFILE_USER_AGENT,
            "Accept": "*/*",
            "Origin": "https://gofile.io",
            "Referer": "https://gofile.io/",
        }

    def _get_response(self, url: str, **kwargs) -> requests.Response | None:
        """发送 HTTP GET 请求，带自动重试。"""
        for _ in range(MAX_RETRIES):
            if self._stop_event.is_set():
                return None
            try:
                return _api_request("GET", url, timeout=TIMEOUT, **kwargs)
            except requests.RequestException:
                continue
        return None

    # -------------------------------------------------------------------------
    # 主流程
    # -------------------------------------------------------------------------

    def run(self) -> list[str] | None:
        """
        执行下载流程。
        返回下载的文件路径列表，失败返回 None。
        """
        # 提取内容 ID
        content_id = self._parse_content_id(self._url)
        if not content_id:
            _log_error(f"gofile: failed to parse content ID from URL: {self._url}")
            return None

        # 检查账户令牌是否获取成功
        if not self._account_token:
            proxy_hint = (
                f"using proxy {PROXY}"
                if PROXY
                else "no proxy configured (set GOFILE_PROXY / ALL_PROXY / HTTPS_PROXY)"
            )
            _log_error(
                f"gofile: no account token — account creation failed " f"({proxy_hint})"
            )
            return None

        # 哈希密码
        hashed_password = (
            sha256(self._password.encode()).hexdigest() if self._password else None
        )

        # 确保输出目录存在
        os.makedirs(self._output_dir, exist_ok=True)

        # 获取内容并构建文件列表
        # 所有文件直接下载到 output_dir，不保留目录结构
        if not self._fetch_and_build(self._output_dir, content_id, hashed_password):
            return None

        if self._stop_event.is_set():
            return None

        if not self._files_info:
            return None

        # 多线程下载
        downloaded_files: list[str] = []
        lock = __import__("threading").Lock()

        def _download_one(info: dict) -> None:
            if self._stop_event.is_set():
                return
            result = self._download_file(info)
            if result:
                with lock:
                    downloaded_files.append(result)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [
                executor.submit(_download_one, info)
                for info in self._files_info.values()
            ]
            for f in futures:
                try:
                    f.result()
                except Exception:
                    pass

        return downloaded_files if downloaded_files else None

    # -------------------------------------------------------------------------
    # 内容解析与获取
    # -------------------------------------------------------------------------

    @staticmethod
    def _parse_content_id(url: str) -> str | None:
        """从 GoFile URL 提取内容 ID。"""
        if not url:
            return None
        candidate = url.strip()
        match = re.search(r"gofile\.io/d/([^/?#\s]+)", candidate, flags=re.IGNORECASE)
        if match:
            return match.group(1)
        # 支持直接传入 content id
        if re.fullmatch(r"[A-Za-z0-9\-]+", candidate):
            return candidate
        return None

    def _fetch_contents(
        self, content_id: str, hashed_password: str | None = None
    ) -> dict | None:
        """
        从 GoFile API 获取内容列表。

        处理 token 窗口回退、rate-limit 退避、以及 error-notPremium 降级。
        """
        params = dict(CONTENTS_QUERY_PARAMS)
        if hashed_password:
            params["password"] = hashed_password

        url = f"https://api.gofile.io/contents/{content_id}"
        window_offsets = [0, -1]  # 当前窗口 → 上一个窗口

        for attempt in range(MAX_RETRIES):
            window_offset = window_offsets[min(attempt, len(window_offsets) - 1)]

            try:
                response = _api_request(
                    "GET",
                    url,
                    headers=self._content_headers(window_offset),
                    params=params,
                    timeout=CONTENT_TIMEOUT,
                )
                data = response.json()
            except requests.Timeout:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(3)
                    continue
                _log_error(
                    f"gofile: content fetch timeout for {content_id} "
                    f"after {MAX_RETRIES} attempts"
                )
                return None
            except Exception as e:
                if _is_edge_block(e):
                    _log_error(
                        "gofile: GoFile API edge blocked the connection "
                        f"(content fetch for {content_id}). "
                        "Try setting GOFILE_PROXY or installing curl_cffi."
                    )
                    return None
                if attempt < MAX_RETRIES - 1:
                    time.sleep(1)
                    continue
                _log_error(
                    f"gofile: content fetch failed for {content_id}: "
                    f"{type(e).__name__}: {e}"
                )
                return None

            status = data.get("status")

            if status == "ok":
                return data

            if status == "error-rateLimit":
                wait = 3 * (attempt + 1)
                if attempt < MAX_RETRIES - 1:
                    time.sleep(wait)
                    continue
                _log_error("gofile: rate limit persisted")
                return None

            if status == "error-notPremium":
                # website token 被拒绝 → 尝试上一个窗口
                if attempt == 0:
                    continue
                _log_error(
                    "gofile: website token rejected (salt may have rotated). "
                    "Set GOFILE_WT_SALT env var."
                )
                return None

            if status == "error-notFound":
                _log_error(f"gofile: content {content_id} not found")
                return None

            _log_error(f"gofile: API error: {data}")
            return None

        return None

    def _fetch_and_build(
        self,
        parent_dir: str,
        content_id: str,
        hashed_password: str | None = None,
        pathing_count: dict[str, int] | None = None,
        file_index: count = count(start=0, step=1),
    ) -> bool:
        """
        递归获取 GoFile 内容元数据并注册文件。

        所有文件扁平化到 parent_dir（不创建子目录结构）。

        Returns:
            True 表示成功获取（即使没有文件），False 表示 API 获取失败。
        """
        data = self._fetch_contents(content_id, hashed_password)
        if data is None:
            self._notify_progress(
                "error", "Failed to fetch content info", None, 0, None, 0
            )
            return False

        content_data = data["data"]

        # 检查密码状态
        password_status = content_data.get("passwordStatus", "passwordOk")
        if password_status != "passwordOk":
            self._notify_progress(
                "error", "Password required or incorrect", None, 0, None, 0
            )
            _log_error(f"gofile: password required or incorrect for {self._url}")
            return False

        if pathing_count is None:
            pathing_count = {}

        # 文件类型：直接注册
        if content_data.get("type") != "folder":
            filepath = self._resolve_collision(
                pathing_count, parent_dir, content_data["name"]
            )
            self._files_info[str(next(file_index))] = {
                "path": os.path.dirname(filepath),
                "filename": os.path.basename(filepath),
                "link": content_data["link"],
            }
            return True

        # 文件夹类型：扁平化递归处理子内容
        children = content_data.get("children") or content_data.get("contents") or {}
        for child in children.values():
            if self._stop_event.is_set():
                return True
            if child["type"] == "folder":
                self._fetch_and_build(
                    parent_dir,
                    child["id"],
                    hashed_password,
                    pathing_count,
                    file_index,
                )
            else:
                filepath = self._resolve_collision(
                    pathing_count, parent_dir, child["name"]
                )
                self._files_info[str(next(file_index))] = {
                    "path": os.path.dirname(filepath),
                    "filename": os.path.basename(filepath),
                    "link": child["link"],
                }

        return True

    # -------------------------------------------------------------------------
    # 文件下载
    # -------------------------------------------------------------------------

    def _download_file(self, file_info: dict[str, str]) -> str | None:
        """下载单个文件，支持断点续传。返回最终文件路径。"""
        filepath = os.path.join(file_info["path"], file_info["filename"])
        filename = file_info["filename"]

        # 跳过已完成的文件
        if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
            self._notify_progress(
                "completed",
                filename,
                os.path.getsize(filepath),
                os.path.getsize(filepath),
                0,
                100,
            )
            return filepath

        tmp_file = f"{filepath}.part"
        url = file_info["link"]

        for attempt in range(MAX_RETRIES):
            if self._stop_event.is_set():
                return None

            headers = {
                "Cookie": f"accountToken={self._account_token}",
                "User-Agent": GOFILE_USER_AGENT,
                "Referer": "https://gofile.io/",
            }
            part_size = 0
            if os.path.isfile(tmp_file):
                part_size = int(os.path.getsize(tmp_file))
                headers["Range"] = f"bytes={part_size}-"

            try:
                response = _api_request(
                    "GET", url, headers=headers, stream=True, timeout=TIMEOUT
                )
                if not response:
                    continue

                with response:
                    status_code = response.status_code
                    if not self._is_valid_status(status_code, part_size):
                        if status_code in (403, 404, 405, 500):
                            continue
                        # 其他非预期状态码，也重试
                        time.sleep(2)
                        continue

                    total_size = self._get_total_size(response.headers, part_size)
                    if total_size is None:
                        continue

                    self._write_chunks(
                        response, tmp_file, part_size, total_size, filename
                    )
                    self._finalize(tmp_file, filepath, total_size, filename)
                    return filepath

            except requests.Timeout:
                if attempt < MAX_RETRIES - 1:
                    continue
                _log_error(f"gofile: download timeout for {filename} after all retries")
            except Exception as e:
                if _is_edge_block(e):
                    _log_error(
                        f"gofile: GoFile API edge blocked download of {filename}"
                    )
                    break
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2)
                    continue
                _log_error(
                    f"gofile: download failed for {filename}: "
                    f"{type(e).__name__}: {e}"
                )

        self._notify_progress("failed", filename, 0, None, 0, 0)
        _log_error(
            f"gofile: download failed after retries: "
            f"{file_info.get('link', '?')} → {filename}"
        )
        return None

    def _write_chunks(
        self,
        response: requests.Response,
        tmp_file: str,
        part_size: int,
        total_size: int,
        filename: str,
    ) -> None:
        """分块写入数据并报告进度。"""
        start_time = time.perf_counter()
        downloaded = part_size
        last_report = 0.0

        with open(tmp_file, "ab") as f:
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                if self._stop_event.is_set():
                    return
                if not chunk:
                    continue

                f.write(chunk)
                downloaded += len(chunk)

                now = time.perf_counter()
                if now - last_report >= 0.5 or downloaded >= total_size:
                    elapsed = now - start_time
                    speed = (downloaded - part_size) / elapsed if elapsed > 0 else 0
                    percent = downloaded / total_size * 100 if total_size else 0
                    self._notify_progress(
                        "downloading", filename, downloaded, total_size, speed, percent
                    )
                    last_report = now

    @staticmethod
    def _finalize(
        tmp_file: str, filepath: str, total_size: int, _filename: str
    ) -> None:
        """校验并完成下载。"""
        if os.path.getsize(tmp_file) == total_size:
            shutil.move(tmp_file, filepath)

    @staticmethod
    def _is_valid_status(status_code: int, part_size: int) -> bool:
        if status_code in (403, 404, 405, 500):
            return False
        if part_size == 0:
            return status_code in (200, 206)
        return status_code == 206

    @staticmethod
    def _get_total_size(
        headers: CaseInsensitiveDict[str], part_size: int
    ) -> int | None:
        if part_size == 0:
            cl = headers.get("Content-Length")
            return int(cl) if cl else None
        cr = headers.get("Content-Range")
        if cr:
            return int(cr.split("/")[-1])
        return None

    @staticmethod
    def _resolve_collision(
        pathing_count: dict[str, int],
        parent_dir: str,
        child_name: str,
        is_dir: bool = False,
    ) -> str:
        """解决命名冲突：同名文件添加 (1), (2) 后缀。"""
        filepath = os.path.join(parent_dir, child_name)
        count_val = pathing_count.get(filepath, 0)
        pathing_count[filepath] = count_val + 1

        if count_val == 0:
            return filepath

        if is_dir:
            return f"{filepath}({count_val})"

        root, ext = os.path.splitext(filepath)
        return f"{root}({count_val}){ext}"

    # -------------------------------------------------------------------------
    # 进度通知
    # -------------------------------------------------------------------------

    def _notify_progress(
        self,
        status: str,
        filename: str,
        downloaded: int,
        total: int | None,
        speed: float,
        percent: float,
    ) -> None:
        """通过回调通知进度。"""
        if self._progress_callback:
            self._progress_callback(
                {
                    "status": status,
                    "filename": filename,
                    "downloaded": downloaded,
                    "total": total,
                    "speed": speed,
                    "percent": percent,
                }
            )


# =============================================================================
# 公开 API
# =============================================================================


def download(
    url: str,
    output_dir: str,
    *,
    password: str | None = None,
    progress_callback: Callable[[dict], None] | None = None,
    stop_event: Event | None = None,
) -> list[str] | None:
    """
    从 GoFile 下载文件。

    参数:
        url:               GoFile 链接 (如 https://gofile.io/d/abc123)
        output_dir:        保存目录
        password:          可选的访问密码
        progress_callback: 进度回调，接收 dict:
                           {status, filename, downloaded, total, speed, percent}
        stop_event:        用于外部取消的 Event

    返回:
        成功时返回下载的文件路径列表，失败返回 None
    """
    downloader = GoFileDownloader(
        url=url,
        output_dir=output_dir,
        password=password,
        progress_callback=progress_callback,
        stop_event=stop_event,
    )
    return downloader.run()
