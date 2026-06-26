#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GoFile 下载器模块（精简版）
==========================
从 GoFile (https://gofile.io) 网盘下载文件。

功能：
  - 支持单个文件/文件夹链接下载
  - 支持密码保护内容
  - 断点续传
  - 多线程并发下载
  - 进度回调

公开 API：
  download(url, output_dir, *, password, progress_callback, stop_event) -> list[str] | None
"""

import os
import time
from hashlib import sha256
from itertools import count
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from typing import Callable

import requests
from requests.structures import CaseInsensitiveDict


# =============================================================================
# 常量
# =============================================================================

CHUNK_SIZE: int = 2 * 1024 * 1024  # 2 MB
MAX_RETRIES: int = 5
TIMEOUT: float = 30.0
MAX_WORKERS: int = 5
PROXY: str = "socks5h://127.0.0.1:7891"


# =============================================================================
# 辅助函数
# =============================================================================

def generate_website_token(user_agent: str, account_token: str = "") -> str:
    """
    生成 GoFile API 需要的动态 X-Website-Token。
    每 4 小时（14400 秒）轮换一次。
    """
    time_slot = int(time.time()) // 14400
    raw = f"{user_agent}::en-US::{account_token}::{time_slot}::9844d94d963d30"
    return sha256(raw.encode()).hexdigest()


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

        # HTTP 会话
        self._session = requests.Session()
        self._session.proxies = {"http": PROXY, "https": PROXY}
        self._session.headers.update({
            "Accept-Encoding": "gzip",
            "User-Agent": "Mozilla/5.0",
            "Connection": "keep-alive",
            "Accept": "*/*",
            "Origin": "https://gofile.io",
            "Referer": "https://gofile.io/",
        })

        # 尝试自动获取账户令牌
        self._setup_account()

    def _setup_account(self) -> None:
        """自动创建匿名 GoFile 账户以获取访问令牌。"""
        user_agent = str(self._session.headers.get("User-Agent", "Mozilla/5.0"))
        wt = generate_website_token(user_agent, "")

        for _ in range(MAX_RETRIES):
            try:
                resp = self._session.post(
                    "https://api.gofile.io/accounts",
                    headers={"X-Website-Token": wt, "X-BL": "en-US"},
                    timeout=TIMEOUT,
                ).json()
                if resp.get("status") == "ok":
                    token = resp["data"]["token"]
                    self._session.cookies.set("Cookie", f"accountToken={token}")
                    self._session.headers.update({"Authorization": f"Bearer {token}"})
                    return
            except Exception:
                continue

    def _get_response(self, **kwargs) -> requests.Response | None:
        """发送 HTTP GET 请求，带自动重试。"""
        for _ in range(MAX_RETRIES):
            try:
                return self._session.get(timeout=TIMEOUT, **kwargs)
            except requests.Timeout:
                continue
        return None

    def run(self) -> list[str] | None:
        """
        执行下载流程。
        返回下载的文件路径列表，失败返回 None。
        """
        # 提取内容 ID
        try:
            if self._url.split("/")[-2] != "d":
                return None
            content_id = self._url.split("/")[-1]
        except IndexError:
            return None

        # 哈希密码
        hashed_password = (
            sha256(self._password.encode()).hexdigest() if self._password else None
        )

        # 确保输出目录存在
        os.makedirs(self._output_dir, exist_ok=True)

        # 构建远程内容树（所有文件直接下载到 output_dir，不保留目录结构）
        self._build_content_tree(self._output_dir, content_id, hashed_password)

        if self._stop_event.is_set():
            return None

        # 如果没有发现任何文件
        if not self._files_info:
            return None

        # 多线程下载
        downloaded_files: list[str] = []
        lock = __import__('threading').Lock()

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
    # 内容树构建
    # -------------------------------------------------------------------------

    def _build_content_tree(
        self,
        parent_dir: str,
        content_id: str,
        password: str | None = None,
        pathing_count: dict[str, int] | None = None,
        file_index: count = count(start=0, step=1),
    ) -> None:
        """递归获取 GoFile 内容元数据，构建本地目录结构并注册文件。"""
        url = (
            f"https://api.gofile.io/contents/{content_id}"
            f"?cache=true&sortField=createTime&sortDirection=1"
        )
        if password:
            url = f"{url}&password={password}"

        if pathing_count is None:
            pathing_count = {}

        # 生成动态令牌
        user_agent = str(self._session.headers.get("User-Agent", "Mozilla/5.0"))
        auth_header = str(self._session.headers.get("Authorization", ""))
        account_token = auth_header.replace("Bearer ", "") if auth_header else ""
        wt = generate_website_token(user_agent, account_token)

        response = self._get_response(
            url=url, headers={"X-Website-Token": wt, "X-BL": "en-US"}
        )
        if not response:
            self._notify_progress("error", "Failed to fetch content info", None, 0, None, 0)
            return

        json_response = response.json()
        if json_response.get("status") != "ok":
            return

        data = json_response["data"]

        # 检查密码状态
        if (
            "password" in data
            and "passwordStatus" in data
            and data["passwordStatus"] != "passwordOk"
        ):
            self._notify_progress("error", "Password required or incorrect", None, 0, None, 0)
            return

        # 文件类型：直接注册
        if data.get("type") != "folder":
            filepath = self._resolve_collision(pathing_count, parent_dir, data["name"])
            self._files_info[str(next(file_index))] = {
                "path": os.path.dirname(filepath),
                "filename": os.path.basename(filepath),
                "link": data["link"],
            }
            return

        # 文件夹类型：扁平化——不创建子目录，递归处理子内容
        # 所有子文件/子文件夹都直接注册到 parent_dir（即 output_dir）
        for child in data.get("children", {}).values():
            if self._stop_event.is_set():
                return
            if child["type"] == "folder":
                self._build_content_tree(
                    parent_dir, child["id"], password, pathing_count, file_index
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

    # -------------------------------------------------------------------------
    # 文件下载
    # -------------------------------------------------------------------------

    def _download_file(self, file_info: dict[str, str]) -> str | None:
        """下载单个文件，支持断点续传。返回最终文件路径。"""
        filepath = os.path.join(file_info["path"], file_info["filename"])
        filename = file_info["filename"]

        # 跳过已完成的文件
        if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
            self._notify_progress("completed", filename, os.path.getsize(filepath),
                                  os.path.getsize(filepath), 0, 100)
            return filepath

        tmp_file = f"{filepath}.part"
        url = file_info["link"]

        for _ in range(MAX_RETRIES):
            if self._stop_event.is_set():
                return None

            headers = {}
            part_size = 0
            if os.path.isfile(tmp_file):
                part_size = int(os.path.getsize(tmp_file))
                headers = {"Range": f"bytes={part_size}-"}

            try:
                response = self._get_response(url=url, headers=headers, stream=True)
                if not response:
                    continue

                with response:
                    status_code = response.status_code
                    if not self._is_valid_status(status_code, part_size):
                        continue

                    total_size = self._get_total_size(response.headers, part_size)
                    if total_size is None:
                        continue

                    self._write_chunks(response, tmp_file, part_size, total_size, filename)
                    self._finalize(tmp_file, filepath, total_size, filename)
                    return filepath

            except requests.Timeout:
                continue

        self._notify_progress("failed", filename, 0, None, 0, 0)
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
                    self._notify_progress("downloading", filename, downloaded,
                                          total_size, speed, percent)
                    last_report = now

    @staticmethod
    def _finalize(tmp_file: str, filepath: str, total_size: int, _filename: str) -> None:
        """校验并完成下载。"""
        if os.path.getsize(tmp_file) == total_size:
            import shutil
            shutil.move(tmp_file, filepath)

    @staticmethod
    def _is_valid_status(status_code: int, part_size: int) -> bool:
        if status_code in (403, 404, 405, 500):
            return False
        if part_size == 0:
            return status_code in (200, 206)
        return status_code == 206

    @staticmethod
    def _get_total_size(headers: CaseInsensitiveDict[str], part_size: int) -> int | None:
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
        pathing_count[filepath] = pathing_count.get(filepath, 0)
        count_val = pathing_count[filepath]

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
            self._progress_callback({
                "status": status,
                "filename": filename,
                "downloaded": downloaded,
                "total": total,
                "speed": speed,
                "percent": percent,
            })


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
