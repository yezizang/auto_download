#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GoFile 下载器（通过 gofile-dl HTTP API 转发）
============================================
不再直连 api.gofile.io（会被其 edge 按 IP / TLS 指纹拦截，这是旧版失效的根因），
而是把下载委托给本机部署的 gofile-dl 容器（ghcr.io/martadams89/gofile-dl，接口见
test/gofile-api.txt）：

  1. POST /start 创建远程任务，gofile-dl 把文件下载到容器 /data
     （即宿主机上的 _HOST_DIR，默认 /opt/gofile-dl/downloads，由 docker 卷映射）。
  2. 轮询 GET /tasks 镜像进度到 progress_callback。
  3. 远程完成后，把 _HOST_DIR 下本次任务产生的文件拍平复制回 output_dir
     （保持旧版扁平化行为，同名文件自动加 (1)(2) 后缀）。

保持旧公开 API 不变（app.py 无需改动）:
  download(url, output_dir, *, password, progress_callback, stop_event) -> list[str] | None
"""

import os
import re
import shutil
import tempfile
import time
from threading import Event
from typing import Callable

import requests

from utils.logger import error as _log_error

# =============================================================================
# 配置（全部可用环境变量覆盖）
# =============================================================================

# gofile-dl 服务地址
BASE_URL: str = os.environ.get("GOFILE_DL_BASE_URL", "http://192.168.3.160:2355")
# HTTP Basic Auth（docker-compose 里的 AUTH_USERNAME / AUTH_PASSWORD）
AUTH_USER: str = os.environ.get("GOFILE_DL_USERNAME", "admin")
AUTH_PASS: str = os.environ.get("GOFILE_DL_PASSWORD", "Abc123!!")
# gofile-dl 容器内下载根目录（POST /start 的 directory 字段，须在 BASE_DIR 内）
REMOTE_DIR: str = os.environ.get("GOFILE_DL_REMOTE_DIR", "/data")
# REMOTE_DIR 映射到的宿主机目录（docker-compose 的 volume 宿主机侧）
HOST_DIR: str = os.environ.get("GOFILE_DL_HOST_DIR", "/opt/gofile-dl/downloads")
# 轮询间隔（秒）
POLL_SECONDS: float = float(os.environ.get("GOFILE_DL_POLL_INTERVAL", "2.0"))

_TIMEOUT: float = 15.0
_CSRF_RE = re.compile(r'name="csrf_token" value="([^"]+)"')


# =============================================================================
# gofile-dl HTTP 客户端
# =============================================================================


class _GofileDlApi:
    """对 gofile-dl 控制面的最小封装：Basic Auth + Flask-WTF CSRF + session cookie。"""

    def __init__(self) -> None:
        self._base = BASE_URL.rstrip("/")
        self._session = requests.Session()
        self._session.auth = (AUTH_USER, AUTH_PASS)
        self._csrf = ""
        self._reload_csrf()

    def _reload_csrf(self) -> None:
        """GET / 拿 session cookie，并从页面解析 CSRF token。"""
        resp = self._session.get(f"{self._base}/", timeout=_TIMEOUT)
        resp.raise_for_status()
        m = _CSRF_RE.search(resp.text)
        if not m:
            raise RuntimeError(
                f"gofile-dl 页面里没找到 CSRF token（{self._base}/）"
            )
        self._csrf = m.group(1)

    def _post(self, path: str, data: dict) -> requests.Response:
        for _ in range(2):  # CSRF 失效时重取一次
            resp = self._session.post(
                f"{self._base}{path}",
                data=data,
                headers={"X-CSRFToken": self._csrf},
                timeout=_TIMEOUT,
            )
            if resp.status_code == 400 and "csrf" in resp.text.lower():
                self._reload_csrf()
                continue
            resp.raise_for_status()
            return resp
        raise RuntimeError(f"gofile-dl POST {path} 失败：CSRF 重试无效")

    def start(self, url: str, password: str | None) -> str:
        payload = {
            "url": url,
            "directory": REMOTE_DIR,
            "incremental": "true",  # 已下载过的文件远程跳过，可续传/增量
        }
        if password:
            payload["password"] = password
        resp = self._post("/start", payload)
        return resp.json()["task_id"]

    def tasks(self) -> dict:
        resp = self._session.get(f"{self._base}/tasks", timeout=_TIMEOUT)
        resp.raise_for_status()
        return resp.json()

    def cancel(self, task_id: str) -> None:
        self._post(f"/cancel/{task_id}", {})

    def remove(self, task_id: str) -> None:
        self._post(f"/remove/{task_id}", {})


# =============================================================================
# 辅助
# =============================================================================


def _host_path(container_path: str) -> str:
    """把容器内文件路径（/data/...）映射为宿主机路径（HOST_DIR/...）。"""
    cp = container_path.replace("\\", "/")
    prefix = REMOTE_DIR.rstrip("/") + "/"
    rel = cp[len(prefix):] if cp.startswith(prefix) else cp.lstrip("/")
    return os.path.join(HOST_DIR, os.path.normpath(rel))


def _dedup_target(path: str, counts: dict[str, int]) -> str:
    """同名目标加 (1)(2) 后缀，行为与旧版一致。"""
    n = counts.get(path, 0)
    counts[path] = n + 1
    if n == 0:
        return path
    stem, ext = os.path.splitext(os.path.basename(path))
    return os.path.join(os.path.dirname(path), f"{stem}({n}){ext}")


def _notify(
    cb: Callable[[dict], None] | None,
    status: str,
    filename: str,
    downloaded: int = 0,
    total: int | None = None,
    speed: float = 0.0,
    percent: float = 0.0,
) -> None:
    if cb:
        cb(
            {
                "status": status,
                "filename": filename,
                "downloaded": downloaded,
                "total": total,
                "speed": speed,
                "percent": percent,
            }
        )


def _emit_progress(task: dict, cb: Callable[[dict], None] | None) -> None:
    """把远程任务字段映射成旧版进度回调格式。"""
    files = task.get("files") or []
    total = sum(f.get("size") or 0 for f in files)
    done = sum(
        (f.get("size") or 0) * max(f.get("progress") or 0, 0) / 100.0
        for f in files
    )
    percent = (done / total * 100.0) if total else float(task.get("overall_progress") or 0)
    _notify(
        cb,
        "downloading",
        task.get("name") or task.get("current_folder") or task.get("url") or "",
        downloaded=int(done),
        total=int(total) if total else None,
        speed=float(task.get("download_speed") or 0),
        percent=min(100.0, percent),
    )


# =============================================================================
# 主流程
# =============================================================================


def _copy_back(task_id: str, task: dict, output_dir: str,
               cb: Callable[[dict], None] | None,
               stop_event: Event) -> list[str] | None:
    """
    远程任务完成后：把 HOST_DIR 下本次任务的文件拍平复制到 output_dir。

    返回复制好的本地文件路径列表；无文件可复制返回 None。
    """
    files = [
        f for f in (task.get("files") or [])
        if f.get("file") and (f.get("progress") or 0) >= 100
    ]
    if not files:
        _log_error(f"gofile: 任务 {task_id} 完成但没有已完成文件记录（分享可能为空）")
        return None

    dst_root = os.path.realpath(output_dir)
    src_root = os.path.realpath(HOST_DIR)
    name = task.get("name") or task_id

    # output_dir 就是 gofile-dl 的宿主目录：结果已在用户指定位置，无需再拷。
    if src_root == dst_root:
        paths = [_host_path(f["file"]) for f in files]
        return paths if all(os.path.isfile(p) for p in paths) else None

    _notify(cb, "downloading", f"正在复制回本地：{name}", percent=99.0)
    os.makedirs(dst_root, exist_ok=True)

    counts: dict[str, int] = {}
    copied: list[str] = []
    for f in files:
        if stop_event.is_set():
            break
        src = _host_path(f["file"])
        if not os.path.isfile(src):
            _log_error(f"gofile: 宿主文件不存在，跳过：{src}")
            continue
        dest = _dedup_target(os.path.join(dst_root, os.path.basename(src)), counts)
        shutil.copy2(src, dest)
        copied.append(dest)

    if stop_event.is_set() or not copied:
        return None

    _notify(cb, "completed", name, percent=100.0)
    return copied


def download(
    url: str,
    output_dir: str,
    *,
    password: str | None = None,
    progress_callback: Callable[[dict], None] | None = None,
    stop_event: Event | None = None,
) -> list[str] | None:
    """
    从 GoFile 下载文件（委托给 gofile-dl 服务）。

    参数:
        url:               GoFile 链接 (如 https://gofile.io/d/abc123)
        output_dir:        本地保存目录（拍平后文件落在这里）
        password:          可选的访问密码
        progress_callback: 进度回调，接收 dict:
                           {status, filename, downloaded, total, speed, percent}
        stop_event:        用于取消的 Event（置位时取消远程任务）

    返回:
        成功返回下载（并拷回本地）的文件路径列表，失败抛 RuntimeError 或返回 None（被取消时）。
    """
    stop_event = stop_event or Event()
    cb = progress_callback

    try:
        api = _GofileDlApi()
    except requests.HTTPError as e:
        raise RuntimeError(f"gofile-dl 连接/鉴权失败（{BASE_URL}）：HTTP {e.response.status_code}") from e
    except requests.RequestException as e:
        raise RuntimeError(f"无法连接 gofile-dl（{BASE_URL}）：{e}") from e

    try:
        task_id = api.start(url, password)
    except requests.RequestException as e:
        raise RuntimeError(f"gofile-dl 提交任务失败：{e}") from e

    _notify(cb, "downloading", url.split("/")[-1], percent=0.0)

    missing = 0
    stall = 0
    last_poll = 0.0

    try:
        while not stop_event.is_set():
            if time.monotonic() - last_poll < POLL_SECONDS:
                time.sleep(0.2)
                continue
            last_poll = time.monotonic()

            try:
                tasks = api.tasks()
            except requests.RequestException as e:
                stall += 1
                if stall >= 3:
                    raise RuntimeError(f"gofile-dl 轮询失败（连续 {stall} 次）：{e}") from e
                continue
            stall = 0

            task = tasks.get(task_id)
            if task is None:
                missing += 1
                if missing >= 30:  # ~1 分钟仍看不到任务，多半服务重启把内存任务清掉了
                    raise RuntimeError(f"gofile-dl 找不到任务 {task_id}（服务可能已重启）")
                continue
            missing = 0

            status = task.get("status")
            if status == "completed":
                result = _copy_back(task_id, task, output_dir, cb, stop_event)
                if result is None:
                    if stop_event.is_set():  # 拷贝途中被取消
                        return None
                    raise RuntimeError(f"gofile-dl 任务完成但未复制到任何文件（{task_id}）")
                try:  # 拷完后把远程任务从 gofile-dl 列表清掉（不删文件）
                    api.remove(task_id)
                except Exception:
                    pass
                return result

            if status == "error":
                emsg = task.get("error_message") or "未知错误"
                raise RuntimeError(f"gofile-dl 任务失败：{emsg}")

            if status == "cancelled":
                _notify(cb, "failed", task.get("name") or task_id)
                return None

            _emit_progress(task, cb)

        # 被外部 stop 了：取消远程任务，返回 None 让上层标为 waiting
        try:
            api.cancel(task_id)
        except Exception:
            pass
        return None

    except RuntimeError:
        raise
    except Exception as e:  # 轮询循环里的意外异常，按任务失败处理
        _log_error(f"gofile: {url} 任务异常：{type(e).__name__}: {e}")
        raise RuntimeError(f"gofile 任务异常：{e}") from e


# =============================================================================
# 自检（无网络）
# =============================================================================


def _selfcheck() -> None:
    """校验宿主路径映射与同名去重逻辑。"""
    global REMOTE_DIR, HOST_DIR
    REMOTE_DIR, HOST_DIR = "/data", "/opt/gofile-dl/downloads"
    assert _host_path("/data/MyShare/a.zip") == os.path.join(
        "/opt/gofile-dl/downloads", "MyShare", "a.zip"
    )
    assert _host_path("/data/file.zip") == os.path.join(
        "/opt/gofile-dl/downloads", "file.zip"
    )

    with tempfile.TemporaryDirectory() as tmp:
        src_dir = os.path.join(tmp, "src")
        dst_dir = os.path.join(tmp, "dst")
        os.makedirs(src_dir)
        with open(os.path.join(src_dir, "a.zip"), "w") as f:
            f.write("x")
        counts: dict[str, int] = {}
        p1 = _dedup_target(os.path.join(dst_dir, "a.zip"), counts)
        p2 = _dedup_target(os.path.join(dst_dir, "a.zip"), counts)
        assert p1 == os.path.join(dst_dir, "a.zip")
        assert p2 == os.path.join(dst_dir, "a(1).zip")
    print("gofile_downloader selfcheck OK")


if __name__ == "__main__":
    _selfcheck()
