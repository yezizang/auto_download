# ! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
自动化下载工具 —— Web 服务
===========================
基于 Flask 的 Web 界面，支持团队多人访问。

启动方式:
  python app.py
  python app.py --port 8080
  python app.py --host 0.0.0.0 --port 5000

然后浏览器打开 http://localhost:5000
"""

import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from threading import Event, Lock
from typing import Callable

from flask import Flask, Response, jsonify, render_template, request

# ---- 导入下载器 ----
from utils.gofile_downloader import download as _gofile_dl
from utils.uploadee_downloader import downloader as _uploadee_dl
from utils.pixeldrain_downloader import download as _pixeldrain_dl
from utils.anonfilesnew_downloader import download as _anonfilesnew_dl
from utils.biteblob_downloader import download as _biteblob_dl
from utils.mediafire_downloader import download as _mediafire_dl
from utils.cloud_mail_ru_downloader import download as _cloud_mail_ru_file_dl
from utils.cloud_mail_ru_downloader import resolve as _cloud_mail_ru_resolve
from utils.transferit_downloader import download as _transferit_dl
from utils.logger import set_log_path, error as _log_error

# =============================================================================
# 配置
# =============================================================================

MAX_WORKERS: int = int(os.environ.get("AUTO_DL_WORKERS", "5"))

# URL 分类规则
SITE_PATTERNS: dict[str, str] = {
    r"gofile\.io/d/": "gofile.io",
    r"upload\.ee/files/": "upload.ee",
    r"pixeldrain\.com/(?:u|l|api/file)/": "pixeldrain.com",
    r"transfer\.it/t/": "transfer.it",
    r"anonfilesnew\.com/": "anonfilesnew.com",
    r"biteblob\.com/": "biteblob.com",
    r"mediafire\.com/file/": "mediafire.com",
    r"cloud\.mail\.ru/public/": "cloud.mail.ru",
}

# 下载器调度表：site → (下载函数, 是否返回列表)
# gofile 返回 list[str]，其余返回 str|None，通过 returns_list 标记统一处理
_DOWNLOADERS: dict[str, tuple[Callable, bool]] = {
    "gofile.io": (_gofile_dl, True),
    "upload.ee": (_uploadee_dl, False),
    "pixeldrain.com": (_pixeldrain_dl, False),
    "transfer.it": (_transferit_dl, False),
    "anonfilesnew.com": (_anonfilesnew_dl, False),
    "biteblob.com": (_biteblob_dl, False),
    "mediafire.com": (_mediafire_dl, False),
    "cloud.mail.ru-file": (_cloud_mail_ru_file_dl, False),
}

# =============================================================================
# 任务状态
# =============================================================================


@dataclass
class TaskInfo:
    index: int
    url: str
    site: str
    status: str = "waiting"
    filename: str = ""
    downloaded: int = 0
    total: int | None = None
    speed: float = 0.0
    percent: float = 0.0
    result_path: str | None = None
    error: str = ""

    def to_dict(self) -> dict:
        return {
            "index": self.index,
            "url": self.url,
            "site": self.site,
            "status": self.status,
            "filename": self.filename,
            "downloaded": self.downloaded,
            "total": self.total,
            "speed": self.speed,
            "percent": round(self.percent, 1),
            "error": self.error,
        }


# =============================================================================
# 全局状态（受 _lock 保护）
# =============================================================================

_lock = Lock()
_tasks: list[TaskInfo] = []  # 所有任务列表（含已完成）
_next_index: int = 1  # 全局递增的任务编号
_seen_urls: set[str] = set()  # 已提交过的 URL（用于去重）
_output_dir: str = ""  # 当前输出目录
_status_message: str = "就绪"

# 下载引擎
_executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
_stop_event = Event()
_active_count: int = 0  # 当前活跃（未完成）的任务数


def _set_status(msg: str) -> None:
    global _status_message
    with _lock:
        _status_message = msg


# 不支持 URL 的统计（不加入任务列表，仅摘要展示）
_unsupported_urls: list[str] = []
_unsupported_count: int = 0


def _get_state() -> dict:
    """线程安全地获取完整状态快照。"""
    with _lock:
        return {
            "tasks": [t.to_dict() for t in _tasks],
            "valid_count": len(_tasks),
            "unsupported_count": _unsupported_count,
            "unsupported_urls": _unsupported_urls[:],
            "output_dir": _output_dir,
            "downloading": _active_count > 0,
            "status_message": _status_message,
        }


# =============================================================================
# URL 分类
# =============================================================================


def classify_url(url: str) -> str | None:
    """根据 URL 判断所属网站。"""
    url_lower = url.strip().lower()
    for pattern, site in SITE_PATTERNS.items():
        if re.search(pattern, url_lower):
            return site
    return None


# =============================================================================
# 下载调度（持久线程池）
# =============================================================================


def _dispatch(task: TaskInfo, stop: Event) -> None:
    """在线程池中执行单个下载任务。"""
    global _active_count

    def cb(info: dict) -> None:
        task.status = info.get("status", task.status)
        task.filename = info.get("filename", task.filename)
        task.downloaded = info.get("downloaded", 0)
        task.total = info.get("total")
        task.speed = info.get("speed", 0)
        task.percent = info.get("percent", 0)

    entry = _DOWNLOADERS.get(task.site)
    if entry is None:
        task.status = "failed"
        task.error = f"未知网站: {task.site}"
        with _lock:
            _active_count -= 1
        return

    download_fn, returns_list = entry

    try:
        task.status = "connecting"
        result = download_fn(
            task.url, _output_dir, progress_callback=cb, stop_event=stop
        )
        if result:
            task.status = "completed"
            task.percent = 100.0
            # gofile 返回 list[str]，其余返回 str
            task.result_path = result[0] if returns_list else result
        else:
            task.status = "failed" if not stop.is_set() else "waiting"
            if task.status == "failed":
                _log_error(f"[{task.site}] download returned empty: {task.url}")
    except Exception as e:
        task.status = "failed"
        task.error = str(e)
        _log_error(f"[{task.site}] {task.url}", exc=e)
    finally:
        with _lock:
            _active_count -= 1

        # 所有任务完成后更新状态
        if _active_count <= 0:
            done = sum(1 for t in _tasks if t.status == "completed")
            fail = sum(1 for t in _tasks if t.status == "failed")
            _set_status(f"完成 — 成功 {done}，失败 {fail}")


def _submit_waiting_tasks() -> int:
    """将所有 waiting 任务提交到持久线程池。返回本次提交的数量。"""
    global _active_count

    submitted = 0
    # ponytail: 整个遍历期间持锁，避免与 api_submit/api_clear 并发竞争
    with _lock:
        for t in _tasks:
            if t.status != "waiting":
                continue
            t.status = "connecting"
            t.filename = ""
            t.downloaded = 0
            t.total = None
            t.speed = 0.0
            t.percent = 0.0
            t.error = ""
            _executor.submit(_dispatch, t, _stop_event)
            _active_count += 1
            submitted += 1

    if submitted > 0:
        _set_status(f"下载中... ({_active_count} 个任务进行中)")

    return submitted


# =============================================================================
# Flask 应用
# =============================================================================

app = Flask(__name__)


@app.route("/")
def index() -> str:
    """主页。"""
    return render_template("index.html")


# ---- API 路由 ----


@app.route("/api/submit", methods=["POST"])
def api_submit():
    """
    提交 URL 列表。自动去重，追加到现有任务列表。
    如果已有下载在进行中，新任务自动开始。
    """
    global _output_dir, _seen_urls, _next_index, _unsupported_urls, _unsupported_count

    data = request.get_json(force=True)
    raw_urls: str = data.get("urls", "").strip()
    output_dir: str = data.get("output_dir", "").strip()

    if not raw_urls:
        return jsonify({"ok": False, "error": "请输入至少一个 URL"})

    if not output_dir:
        output_dir = os.path.join(os.getcwd(), "downloads")

    os.makedirs(output_dir, exist_ok=True)
    set_log_path(os.path.join(output_dir, "errors.log"))

    with _lock:
        _output_dir = output_dir

    # 解析并去重
    lines = [line.strip() for line in raw_urls.splitlines() if line.strip()]

    # 第一遍：分类 + 去重（不持锁，cloud.mail.ru 需外部分辨）
    new_supported: list[TaskInfo] = []
    new_unsupported_urls: list[str] = []
    skipped_dup = 0

    for url in lines:
        normalized = url.lower().rstrip("/")
        with _lock:
            if normalized in _seen_urls:
                skipped_dup += 1
                continue
            _seen_urls.add(normalized)

        site = classify_url(url)

        # cloud.mail.ru 文件夹 → 展开为 N 个单文件任务
        if site == "cloud.mail.ru":
            try:
                resolved = _cloud_mail_ru_resolve(url)
            except Exception:
                new_unsupported_urls.append(url)
                continue

            if not resolved:
                new_unsupported_urls.append(url)
                continue

            for file_info in resolved:
                task = TaskInfo(
                    index=_next_index,
                    url=file_info["url"],
                    site="cloud.mail.ru-file",
                    filename=file_info["filename"],
                )
                new_supported.append(task)
                _next_index += 1
        elif site:
            task = TaskInfo(index=_next_index, url=url, site=site)
            new_supported.append(task)
            _next_index += 1
        else:
            new_unsupported_urls.append(url)

    with _lock:
        _unsupported_urls.extend(new_unsupported_urls)
        _unsupported_count += len(new_unsupported_urls)
        _tasks.extend(new_supported)

    total_new = len(new_supported) + len(new_unsupported_urls)

    msg_parts = [f"新增 {total_new} 个"]
    if len(new_supported) > 0:
        msg_parts.append(f"可下载 {len(new_supported)} 个")
    if len(new_unsupported_urls) > 0:
        msg_parts.append(f"不支持 {len(new_unsupported_urls)} 个")
    if skipped_dup > 0:
        msg_parts.append(f"跳过重复 {skipped_dup} 个")
    _set_status("，".join(msg_parts))

    # 如果当前有下载在进行中，自动提交新任务
    new_submitted = 0
    with _lock:
        was_active = _active_count > 0
    if was_active:
        new_submitted = _submit_waiting_tasks()

    return jsonify(
        {
            "ok": True,
            "total_new": total_new,
            "supported_new": len(new_supported),
            "unsupported_new": len(new_unsupported_urls),
            "skipped_dup": skipped_dup,
            "auto_started": new_submitted,
            "unsupported_urls": new_unsupported_urls,
            "output_dir": output_dir,
        }
    )


@app.route("/api/start", methods=["POST"])
def api_start():
    """开始/继续下载所有等待中的任务。"""
    global _stop_event

    # 如果旧 stop_event 已被设置（之前被 stop 了），创建新的
    if _stop_event.is_set():
        _stop_event = Event()

    submitted = _submit_waiting_tasks()

    if submitted == 0:
        return jsonify({"ok": False, "error": "没有等待下载的任务"})

    return jsonify({"ok": True, "message": f"已开始 {submitted} 个任务"})


@app.route("/api/stop", methods=["POST"])
def api_stop():
    """停止所有进行中的下载。"""
    global _active_count
    _stop_event.set()
    # 下次 start 会创建新的 Event
    with _lock:
        _active_count = 0
    _set_status("已停止")
    return jsonify({"ok": True, "message": "停止信号已发送"})


@app.route("/api/retry", methods=["POST"])
def api_retry():
    """重试失败任务。传 index 重试单个，否则重试全部失败。"""
    global _stop_event

    data = request.get_json(silent=True) or {}
    target_index = data.get("index")
    error_msg = None

    with _lock:
        if target_index is not None:
            for t in _tasks:
                if t.index == target_index and t.status == "failed":
                    t.status = "waiting"
                    t.error = ""
                    break
            else:
                error_msg = f"未找到可重试的任务 #{target_index}"
        else:
            count = sum(1 for t in _tasks if t.status == "failed")
            if count == 0:
                error_msg = "没有失败的任务"
            else:
                for t in _tasks:
                    if t.status == "failed":
                        t.status = "waiting"
                        t.error = ""

    if error_msg:
        return jsonify({"ok": False, "error": error_msg})

    if _stop_event.is_set():
        _stop_event = Event()

    submitted = _submit_waiting_tasks()
    return jsonify({"ok": True, "message": f"已重新开始 {submitted} 个任务"})


@app.route("/api/clear", methods=["POST"])
def api_clear():
    """清空已完成和失败的任务记录。"""
    global _tasks
    with _lock:
        # 仅保留 waiting/connecting/downloading 的任务
        _tasks = [
            t for t in _tasks if t.status in ("waiting", "connecting", "downloading")
        ]
    _set_status("已清空已完成的任务")
    return jsonify({"ok": True, "message": "已清空"})


@app.route("/api/status")
def api_status():
    """获取当前完整状态（JSON）。"""
    return jsonify(_get_state())


@app.route("/api/stream")
def api_stream():
    """
    Server-Sent Events (SSE) 端点。
    前端通过 EventSource 连接，实时接收状态更新。
    """

    def generate():
        last_hash = ""
        while True:
            state = _get_state()
            state_json = json.dumps(state, ensure_ascii=False, default=str)
            new_hash = str(hash(state_json))
            if new_hash != last_hash:
                last_hash = new_hash
                yield f"data: {state_json}\n\n"
            time.sleep(0.5)

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


# =============================================================================
# 启动
# =============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="自动化下载工具 Web 服务")
    parser.add_argument("--host", default="0.0.0.0", help="监听地址 (默认 0.0.0.0)")
    parser.add_argument("--port", type=int, default=5000, help="监听端口 (默认 5000)")
    parser.add_argument("--debug", action="store_true", help="调试模式")
    args = parser.parse_args()

    print(f"""
╔══════════════════════════════════════════════╗
║      🚀 自动化下载工具 - Web 服务            ║
╠══════════════════════════════════════════════╣
║  支持: gofile.io | upload.ee | pixeldrain   ║
║        mediafire | transfer.it | anonfilesnew║
║        biteblob | cloud.mail.ru             ║
║  地址: http://{args.host}:{args.port}                  ║
║  线程: {MAX_WORKERS}                                ║
╚══════════════════════════════════════════════╝
""")

    app.run(host=args.host, port=args.port, debug=args.debug, threaded=True)
