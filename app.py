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

import atexit
import json
import os
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from threading import Event, Lock
from typing import Callable

from flask import Flask, Response, jsonify, render_template, request, send_from_directory

from utils.proxy import ensure_library_proxy, load_dotenv

load_dotenv()
ensure_library_proxy()

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
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
# 环境变量只给测试改位置。页面不能指定项目外的目录。
DOWNLOADS_ROOT = os.environ.get("AUTO_DL_DOWNLOADS") or os.path.join(PROJECT_ROOT, "downloads")
STATE_PATH = os.environ.get("AUTO_DL_STATE") or os.path.join(PROJECT_ROOT, "data", "state.json")
MAX_URLS_PER_SUBMIT = 300

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

_URL_RE = re.compile(r"https?://[^\s<>\"'`]+", re.IGNORECASE)
_ACTIVE = ("connecting", "downloading")
_RESUMABLE = ("waiting", "stopped")

# =============================================================================
# 任务状态
# =============================================================================


@dataclass
class TaskInfo:
    index: int
    url: str
    site: str
    source_url: str = ""
    status: str = "waiting"
    filename: str = ""
    downloaded: int = 0
    total: int | None = None
    speed: float = 0.0
    percent: float = 0.0
    result_path: str | None = None
    error: str = ""
    batch_id: str = ""
    output_rel: str = ""
    password: str = ""
    stop: Event = field(default_factory=Event)

    def to_dict(self) -> dict:
        rel = _result_rel(self.result_path)
        ready = bool(rel and self.result_path and os.path.isfile(self.result_path))
        return {
            "index": self.index,
            "url": self.url,
            "site": self.site,
            "batch_id": self.batch_id,
            "status": self.status,
            "filename": self.filename,
            "downloaded": self.downloaded,
            "total": self.total,
            "speed": self.speed,
            "percent": round(self.percent, 1),
            "error": self.error,
            "output_dir": _display_dir(self.output_rel),
            "result_path": rel if ready or rel else "",
            "result_name": self.filename or (os.path.basename(rel) if rel else ""),
            "file_ready": ready,
        }


def _task_record(task: TaskInfo) -> dict:
    """写入磁盘的字段。密码和停止事件只留在内存里。"""
    return {
        "index": task.index,
        "url": task.url,
        "site": task.site,
        "source_url": task.source_url,
        "status": task.status,
        "filename": task.filename,
        "downloaded": task.downloaded,
        "total": task.total,
        "percent": task.percent,
        "result_path": task.result_path or "",
        "error": task.error,
        "batch_id": task.batch_id,
        "output_rel": task.output_rel,
    }


# =============================================================================
# 全局状态（受 _lock 保护）
# =============================================================================

_lock = Lock()
_io_lock = Lock()
_tasks: list[TaskInfo] = []
_next_index: int = 1
_seen_urls: set[str] = set()
_status_message: str = "就绪"
_executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
_active_count: int = 0
_unsupported: list[dict] = []
_batches: dict[str, dict] = {}


def _normalize(url: str) -> str:
    return url.strip().lower().rstrip("/")


def extract_urls(text: str) -> list[str]:
    """从整段文字里抽出 http(s) 链接，去掉贴在末尾的标点。"""
    found: list[str] = []
    for raw in _URL_RE.findall(text or ""):
        url = raw.rstrip(").,;，。、！!？?]>\"'")
        if url:
            found.append(url)
    return found


def classify_url(url: str) -> str | None:
    """根据 URL 判断所属网站。"""
    url_lower = url.strip().lower()
    for pattern, site in SITE_PATTERNS.items():
        if re.search(pattern, url_lower):
            return site
    return None


def _display_dir(rel: str) -> str:
    rel = (rel or "").strip("/")
    return "downloads" if not rel else f"downloads/{rel}"


def _downloads_root() -> str:
    os.makedirs(DOWNLOADS_ROOT, exist_ok=True)
    return os.path.realpath(DOWNLOADS_ROOT)


def _is_under(root: str, path: str) -> bool:
    root_real = os.path.realpath(root) if os.path.exists(root) else os.path.abspath(root)
    path_real = os.path.realpath(path) if os.path.exists(path) else os.path.abspath(path)
    try:
        return os.path.commonpath([root_real, path_real]) == root_real
    except ValueError:
        return False


def _safe_batch_dir(subdir: str, *, create: bool) -> str:
    """返回 downloads 内的相对目录。拒绝绝对路径、盘符和 .. 。"""
    subdir = (subdir or "").strip().replace("\\", "/").strip("/")
    parts = [time.strftime("%Y-%m-%d")]
    if subdir:
        for part in subdir.split("/"):
            if part in ("", "."):
                continue
            if (
                part == ".."
                or ":" in part
                or part.startswith("~")
                or re.search(r"[\x00-\x1f]", part)
                or part.endswith((" ", "."))
                or len(part) > 80
            ):
                raise ValueError("子目录不合法，不能包含 .. 或绝对路径")
            parts.append(part)
    root = _downloads_root()
    dest = os.path.abspath(os.path.join(root, *parts))
    if not _is_under(root, dest):
        raise ValueError("保存位置必须在 downloads 目录内")
    if create:
        os.makedirs(dest, exist_ok=True)
        if not _is_under(root, dest):
            raise ValueError("保存位置必须在 downloads 目录内")
    return "/".join(parts)


def _abs_from_rel(rel: str) -> str | None:
    rel = (rel or "").strip().replace("\\", "/").strip("/")
    parts = rel.split("/")
    if not rel or any(part in ("", "..") or ":" in part or part.startswith("~") for part in parts):
        return None
    root = _downloads_root()
    dest = os.path.abspath(os.path.join(root, *parts))
    if not _is_under(root, dest):
        return None
    return dest


def _result_rel(path: str | None) -> str:
    if not path:
        return ""
    root = _downloads_root()
    if not _is_under(root, path):
        return ""
    real = os.path.realpath(path) if os.path.exists(path) else os.path.abspath(path)
    return os.path.relpath(real, os.path.realpath(root)).replace("\\", "/")


def _ensure_log() -> None:
    os.makedirs(DOWNLOADS_ROOT, exist_ok=True)
    set_log_path(os.path.join(DOWNLOADS_ROOT, "errors.log"))


def _snapshot_unlocked() -> dict:
    return {
        "next_index": _next_index,
        "tasks": [_task_record(t) for t in _tasks],
        "unsupported": _unsupported,
        "batches": _batches,
    }


def _persist() -> None:
    try:
        with _lock:
            payload = _snapshot_unlocked()
        folder = os.path.dirname(STATE_PATH)
        os.makedirs(folder, exist_ok=True)
        text = json.dumps(payload, ensure_ascii=False, indent=2)
        tmp = STATE_PATH + ".tmp"
        with _io_lock:
            with open(tmp, "w", encoding="utf-8") as handle:
                handle.write(text)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, STATE_PATH)
    except Exception as exc:
        _log_error("保存任务状态失败", exc=exc)


def _load_state() -> None:
    """重启后恢复任务。正在下载的改为等待，不会自动开下。"""
    global _tasks, _next_index, _seen_urls, _unsupported, _batches
    if not os.path.isfile(STATE_PATH):
        return
    try:
        with open(STATE_PATH, encoding="utf-8") as handle:
            data = json.load(handle)
    except Exception as exc:
        _log_error("读取任务状态失败", exc=exc)
        return

    tasks: list[TaskInfo] = []
    for raw in data.get("tasks") or []:
        if not isinstance(raw, dict):
            continue
        status = raw.get("status") or "waiting"
        if status in _ACTIVE:
            status = "waiting"
        try:
            index = int(raw.get("index") or 0)
        except (TypeError, ValueError):
            continue
        total = raw.get("total")
        try:
            total = int(total) if total is not None else None
        except (TypeError, ValueError):
            total = None
        tasks.append(
            TaskInfo(
                index=index,
                url=str(raw.get("url") or ""),
                site=str(raw.get("site") or ""),
                source_url=str(raw.get("source_url") or ""),
                status=status,
                filename=str(raw.get("filename") or ""),
                downloaded=int(raw.get("downloaded") or 0),
                total=total,
                percent=float(raw.get("percent") or 0),
                result_path=str(raw.get("result_path") or "") or None,
                error=str(raw.get("error") or ""),
                batch_id=str(raw.get("batch_id") or ""),
                output_rel=str(raw.get("output_rel") or ""),
            )
        )

    batches = data.get("batches") if isinstance(data.get("batches"), dict) else {}
    unsupported = []
    for item in data.get("unsupported") or []:
        if isinstance(item, dict) and item.get("url"):
            unsupported.append(
                {
                    "url": str(item.get("url")),
                    "reason": str(item.get("reason") or "无法识别的网站"),
                    "batch_id": str(item.get("batch_id") or ""),
                }
            )

    with _lock:
        _tasks = tasks
        _unsupported = unsupported
        _batches = {str(k): v for k, v in batches.items() if isinstance(v, dict)}
        _seen_urls = {t.source_url for t in _tasks if t.source_url}
        highest = max((t.index for t in _tasks), default=0)
        try:
            stored = int(data.get("next_index") or 1)
        except (TypeError, ValueError):
            stored = 1
        _next_index = max(stored, highest + 1)
        _refresh_status_unlocked()


def _refresh_status_unlocked() -> None:
    global _status_message
    if _active_count > 0:
        _status_message = f"下载中... ({_active_count} 个任务进行中)"
        return
    if not _tasks and not _unsupported:
        _status_message = "就绪"
        return
    done = sum(t.status == "completed" for t in _tasks)
    fail = sum(t.status == "failed" for t in _tasks)
    stopped = sum(t.status == "stopped" for t in _tasks)
    waiting = sum(t.status == "waiting" for t in _tasks)
    parts = [f"成功 {done}，失败 {fail}"]
    if stopped:
        parts.append(f"已停止 {stopped}")
    if waiting:
        parts.append(f"等待 {waiting}")
    _status_message = "，".join(parts)


def _refresh_status() -> None:
    with _lock:
        _refresh_status_unlocked()


def _batches_public_unlocked() -> list[dict]:
    rows = []
    for bid, info in _batches.items():
        rows.append(
            {
                "id": bid,
                "output_dir": _display_dir(str(info.get("output_rel") or "")),
                "created": info.get("created") or 0,
                "task_count": sum(1 for t in _tasks if t.batch_id == bid),
            }
        )
    rows.sort(key=lambda row: row["created"], reverse=True)
    return rows


def _get_state() -> dict:
    with _lock:
        return {
            "tasks": [t.to_dict() for t in _tasks],
            "valid_count": len(_tasks),
            "unsupported_count": len(_unsupported),
            "unsupported": [dict(item) for item in _unsupported],
            "downloading": _active_count > 0,
            "status_message": _status_message,
            "workers": MAX_WORKERS,
            "today_dir": _display_dir(time.strftime("%Y-%m-%d")),
            "batches": _batches_public_unlocked(),
        }


def _find_unlocked(index: int) -> TaskInfo | None:
    for task in _tasks:
        if task.index == index:
            return task
    return None


def _release_source_unlocked(source_url: str) -> None:
    if source_url and not any(t.source_url == source_url for t in _tasks):
        _seen_urls.discard(source_url)


def _prune_batches_unlocked() -> None:
    used = {t.batch_id for t in _tasks} | {item.get("batch_id") for item in _unsupported}
    for bid in list(_batches):
        if bid not in used:
            del _batches[bid]


def _batch_known_unlocked(batch_id: str) -> bool:
    return batch_id in _batches or any(t.batch_id == batch_id for t in _tasks)


def _set_current(task: TaskInfo, stop: Event, status: str, **fields) -> bool:
    with _lock:
        if task.stop is not stop:
            return False
        task.status = status
        for key, value in fields.items():
            setattr(task, key, value)
        return True


# =============================================================================
# 下载调度（持久线程池）
# =============================================================================


def _dispatch(task: TaskInfo, stop: Event) -> None:
    """在线程池中执行单个下载任务。stop 只属于这一次执行。"""
    global _active_count

    def cb(info: dict) -> None:
        if task.stop is not stop:
            return
        task.status = info.get("status", task.status)
        task.filename = info.get("filename", task.filename)
        task.downloaded = info.get("downloaded", 0)
        task.total = info.get("total")
        task.speed = info.get("speed", 0)
        task.percent = info.get("percent", 0)

    try:
        if stop.is_set():
            _set_current(task, stop, "stopped", error="", speed=0.0)
            return

        entry = _DOWNLOADERS.get(task.site)
        if entry is None:
            _set_current(task, stop, "failed", error=f"未知网站: {task.site}")
            return

        out = _abs_from_rel(task.output_rel)
        if out is None:
            _set_current(task, stop, "failed", error="保存目录不可用")
            return
        os.makedirs(out, exist_ok=True)

        download_fn, returns_list = entry
        kwargs = {}
        if task.site == "gofile.io" and task.password:
            kwargs["password"] = task.password
        result = download_fn(task.url, out, progress_callback=cb, stop_event=stop, **kwargs)
        if task.stop is not stop:
            return

        path = None
        if returns_list:
            if isinstance(result, list) and result:
                path = result[0]
        elif isinstance(result, str) and result:
            path = result

        if path:
            name = os.path.basename(path) or task.filename
            _set_current(
                task,
                stop,
                "completed",
                percent=100.0,
                speed=0.0,
                result_path=path,
                filename=name,
                error="",
            )
        elif stop.is_set():
            _set_current(task, stop, "stopped", error="", speed=0.0)
        else:
            _set_current(task, stop, "failed", error=task.error or "下载失败", speed=0.0)
            _log_error(f"[{task.site}] download returned empty: {task.url}")
    except Exception as exc:
        if stop.is_set():
            _set_current(task, stop, "stopped", error="", speed=0.0)
        else:
            _set_current(task, stop, "failed", error=str(exc), speed=0.0)
            _log_error(f"[{task.site}] {task.url}", exc=exc)
    finally:
        with _lock:
            _active_count = max(0, _active_count - 1)
            current = task.stop is stop
            _refresh_status_unlocked()
        if current:
            _persist()


def _submit_specific(tasks: list[TaskInfo]) -> int:
    """只提交给出的等待/已停止任务，不会带上其他批次。"""
    global _active_count

    armed: list[tuple[TaskInfo, Event]] = []
    with _lock:
        for task in tasks:
            if task.status not in _RESUMABLE:
                continue
            task.stop = Event()
            task.status = "connecting"
            task.downloaded = 0
            task.total = None
            task.speed = 0.0
            task.percent = 0.0
            task.error = ""
            _active_count += 1
            armed.append((task, task.stop))
        _refresh_status_unlocked()

    for task, stop in armed:
        _executor.submit(_dispatch, task, stop)
    if armed:
        _persist()
    return len(armed)


def _apply_password(task: TaskInfo, password: str) -> None:
    if task.site == "gofile.io" and password:
        task.password = password


# =============================================================================
# Flask 应用
# =============================================================================

app = Flask(__name__)


@app.route("/")
def index() -> str:
    """主页。"""
    return render_template(
        "index.html",
        workers=MAX_WORKERS,
        today_dir=_display_dir(time.strftime("%Y-%m-%d")),
    )


def _json_body() -> dict:
    data = request.get_json(force=True, silent=True)
    return data if isinstance(data, dict) else {}


def _require_batch(data: dict) -> tuple[str | None, str | None]:
    batch_id = str(data.get("batch_id") or "").strip()
    if not batch_id:
        return None, "请选择要操作的批次"
    with _lock:
        if not _batch_known_unlocked(batch_id):
            return None, "这个批次不存在"
    return batch_id, None


@app.route("/api/submit", methods=["POST"])
def api_submit():
    """从粘贴文本提取链接，追加为一个新批次并立刻开始。"""
    global _next_index

    data = _json_body()
    raw = data.get("urls")
    if not isinstance(raw, str) or not raw.strip():
        return jsonify({"ok": False, "error": "请粘贴至少一段包含链接的文字"})
    subdir = data.get("subdir") if isinstance(data.get("subdir"), str) else ""
    password = data.get("gofile_password")
    password = password.strip() if isinstance(password, str) else ""

    urls = extract_urls(raw)
    if not urls:
        return jsonify({"ok": False, "error": "没有找到可用的链接"})
    if len(urls) > MAX_URLS_PER_SUBMIT:
        return jsonify({"ok": False, "error": f"一次最多提交 {MAX_URLS_PER_SUBMIT} 个链接"})

    try:
        output_rel = _safe_batch_dir(subdir, create=False)
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)})

    batch_id = uuid.uuid4().hex[:8]
    new_supported: list[TaskInfo] = []
    new_unsupported: list[dict] = []
    skipped_dup = 0

    for url in urls:
        normalized = _normalize(url)
        with _lock:
            if normalized in _seen_urls:
                skipped_dup += 1
                continue

        site = classify_url(url)
        if site == "cloud.mail.ru":
            try:
                resolved = _cloud_mail_ru_resolve(url)
            except Exception as exc:
                new_unsupported.append(
                    {"url": url, "reason": f"解析失败：{exc}", "batch_id": batch_id}
                )
                continue
            if not resolved:
                new_unsupported.append(
                    {"url": url, "reason": "文件夹为空或没有可下载的文件", "batch_id": batch_id}
                )
                continue
            with _lock:
                if normalized in _seen_urls:
                    skipped_dup += 1
                    continue
                _seen_urls.add(normalized)
                for file_info in resolved:
                    new_supported.append(
                        TaskInfo(
                            index=_next_index,
                            url=file_info["url"],
                            site="cloud.mail.ru-file",
                            source_url=normalized,
                            filename=file_info.get("filename") or "",
                            batch_id=batch_id,
                            output_rel=output_rel,
                        )
                    )
                    _next_index += 1
        elif site:
            with _lock:
                if normalized in _seen_urls:
                    skipped_dup += 1
                    continue
                _seen_urls.add(normalized)
                task = TaskInfo(
                    index=_next_index,
                    url=url,
                    site=site,
                    source_url=normalized,
                    batch_id=batch_id,
                    output_rel=output_rel,
                )
                _apply_password(task, password)
                new_supported.append(task)
                _next_index += 1
        else:
            new_unsupported.append(
                {"url": url, "reason": "无法识别的网站", "batch_id": batch_id}
            )

    if new_supported:
        try:
            output_rel = _safe_batch_dir(subdir, create=True)
        except ValueError as exc:
            with _lock:
                for task in new_supported:
                    _seen_urls.discard(task.source_url)
            return jsonify({"ok": False, "error": str(exc)})
        for task in new_supported:
            task.output_rel = output_rel

    if not new_supported and not new_unsupported:
        return jsonify(
            {
                "ok": True,
                "batch_id": "",
                "added": 0,
                "skipped": skipped_dup,
                "unsupported": 0,
                "unsupported_items": [],
                "output_dir": "",
                "auto_started": 0,
            }
        )

    with _lock:
        _batches[batch_id] = {"output_rel": output_rel, "created": time.time()}
        _unsupported.extend(new_unsupported)
        _tasks.extend(new_supported)
        _refresh_status_unlocked()

    _ensure_log()
    _persist()
    started = _submit_specific(new_supported) if new_supported else 0

    return jsonify(
        {
            "ok": True,
            "batch_id": batch_id,
            "added": len(new_supported),
            "skipped": skipped_dup,
            "unsupported": len(new_unsupported),
            "unsupported_items": new_unsupported,
            "output_dir": _display_dir(output_rel),
            "auto_started": started,
        }
    )


@app.route("/api/start", methods=["POST"])
def api_start():
    """继续所选批次里等待或已停止的任务。"""
    data = _json_body()
    batch_id, error = _require_batch(data)
    if error:
        return jsonify({"ok": False, "error": error})
    password = data.get("gofile_password")
    password = password.strip() if isinstance(password, str) else ""

    with _lock:
        chosen = [t for t in _tasks if t.batch_id == batch_id and t.status in _RESUMABLE]
        for task in chosen:
            _apply_password(task, password)
    if not chosen:
        return jsonify({"ok": False, "error": "这个批次没有可继续的任务"})

    submitted = _submit_specific(chosen)
    return jsonify({"ok": True, "message": f"已继续 {submitted} 个任务"})


@app.route("/api/stop", methods=["POST"])
def api_stop():
    """停止所选批次。其他批次继续下载。"""
    batch_id, error = _require_batch(_json_body())
    if error:
        return jsonify({"ok": False, "error": error})

    with _lock:
        hit = 0
        for task in _tasks:
            if task.batch_id != batch_id:
                continue
            if task.status == "waiting":
                task.status = "stopped"
                task.speed = 0.0
                hit += 1
            elif task.status in _ACTIVE:
                task.stop.set()
                hit += 1
        _refresh_status_unlocked()
    _persist()
    if hit == 0:
        return jsonify({"ok": False, "error": "这个批次没有正在下载的任务"})
    return jsonify({"ok": True, "message": "已停止这个批次"})


@app.route("/api/retry", methods=["POST"])
def api_retry():
    """重试一个任务，或重试所选批次里的全部失败任务。"""
    data = _json_body()
    password = data.get("gofile_password")
    password = password.strip() if isinstance(password, str) else ""
    target = data.get("index")
    chosen: list[TaskInfo] = []

    if target is not None:
        try:
            index = int(target)
        except (TypeError, ValueError):
            return jsonify({"ok": False, "error": "任务编号不正确"})
        with _lock:
            task = _find_unlocked(index)
            if task is None or task.status not in ("failed", "stopped"):
                return jsonify({"ok": False, "error": f"未找到可重试的任务 #{index}"})
            task.status = "waiting"
            _apply_password(task, password)
            chosen = [task]
    else:
        batch_id, error = _require_batch(data)
        if error:
            return jsonify({"ok": False, "error": error})
        with _lock:
            chosen = [t for t in _tasks if t.batch_id == batch_id and t.status == "failed"]
            for task in chosen:
                task.status = "waiting"
                _apply_password(task, password)
        if not chosen:
            return jsonify({"ok": False, "error": "这个批次没有失败的任务"})

    submitted = _submit_specific(chosen)
    return jsonify({"ok": True, "message": f"已重新开始 {submitted} 个任务"})


@app.route("/api/cancel", methods=["POST"])
def api_cancel():
    """取消一个任务。正在下载的只停这一个，等待中的改为已停止。"""
    data = _json_body()
    try:
        index = int(data.get("index"))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "任务编号不正确"})

    with _lock:
        task = _find_unlocked(index)
        if task is None:
            return jsonify({"ok": False, "error": f"未找到任务 #{index}"})
        if task.status == "waiting":
            task.status = "stopped"
            task.speed = 0.0
        elif task.status in _ACTIVE:
            task.stop.set()
        elif task.status == "stopped":
            return jsonify({"ok": True, "message": "任务已经停止"})
        else:
            return jsonify({"ok": False, "error": "这个任务已经结束，可以删除"})
        _refresh_status_unlocked()
    _persist()
    return jsonify({"ok": True, "message": f"已取消任务 #{index}"})


@app.route("/api/remove", methods=["POST"])
def api_remove():
    """从列表删除一个已经结束的任务。"""
    global _tasks
    data = _json_body()
    try:
        index = int(data.get("index"))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "任务编号不正确"})

    with _lock:
        task = _find_unlocked(index)
        if task is None:
            return jsonify({"ok": False, "error": f"未找到任务 #{index}"})
        if task.status in _ACTIVE:
            return jsonify({"ok": False, "error": "任务还在下载，请先取消"})
        _tasks = [t for t in _tasks if t.index != index]
        _release_source_unlocked(task.source_url)
        _prune_batches_unlocked()
        _refresh_status_unlocked()
    _persist()
    return jsonify({"ok": True, "message": f"已删除任务 #{index}"})


@app.route("/api/clear", methods=["POST"])
def api_clear():
    """只清除所选批次里已完成的任务，失败和未完成的保留。"""
    global _tasks
    batch_id, error = _require_batch(_json_body())
    if error:
        return jsonify({"ok": False, "error": error})

    with _lock:
        removed = [
            t for t in _tasks if t.batch_id == batch_id and t.status == "completed"
        ]
        if not removed:
            return jsonify({"ok": False, "error": "这个批次没有已完成的任务"})
        drop = {t.index for t in removed}
        _tasks = [t for t in _tasks if t.index not in drop]
        for task in removed:
            _release_source_unlocked(task.source_url)
        _prune_batches_unlocked()
        _refresh_status_unlocked()
    _persist()
    return jsonify({"ok": True, "message": f"已清除 {len(removed)} 个已完成任务"})


@app.route("/api/status")
def api_status():
    """获取当前完整状态（JSON）。"""
    return jsonify(_get_state())


@app.route("/api/files")
def api_files():
    """列出 downloads 里的文件。不列出目录之外的路径。"""
    query = (request.args.get("q") or "").strip().lower()
    root = _downloads_root()
    items = []
    truncated = False
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [name for name in dirnames if name not in {".", ".."}]
        for name in filenames:
            if name == "errors.log":
                continue
            full = os.path.join(dirpath, name)
            real = _result_rel(full)
            if not real or not os.path.isfile(full):
                continue
            if query and query not in real.lower() and query not in name.lower():
                continue
            try:
                stat = os.stat(full)
            except OSError:
                continue
            items.append(
                {
                    "name": name,
                    "path": real,
                    "size": stat.st_size,
                    "mtime": int(stat.st_mtime),
                }
            )
    items.sort(key=lambda item: item["mtime"], reverse=True)
    if len(items) > 400:
        items = items[:400]
        truncated = True
    return jsonify({"ok": True, "files": items, "truncated": truncated})


@app.route("/api/files/download")
def api_file_download():
    """把 downloads 里的一个文件发给浏览器。"""
    rel = request.args.get("path") or ""
    dest = _abs_from_rel(rel)
    if dest is None or not os.path.isfile(dest) or not _is_under(_downloads_root(), dest):
        return jsonify({"ok": False, "error": "文件不存在"}), 404
    return send_from_directory(
        os.path.dirname(dest),
        os.path.basename(dest),
        as_attachment=True,
        download_name=os.path.basename(dest),
    )


@app.route("/api/stream")
def api_stream():
    """Server-Sent Events。前端用 EventSource 接收状态。"""

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


_ensure_log()
_load_state()
atexit.register(_persist)


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
