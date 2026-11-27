# ! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""进程代理：从 .env 和标准环境变量读取，供各下载器共用。"""

import os

_LOADED = False


def load_dotenv() -> None:
    """读取项目根目录的 .env。已存在的环境变量不覆盖。"""
    global _LOADED
    if _LOADED:
        return
    _LOADED = True
    path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
    if not os.path.isfile(path):
        return
    with open(path, encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


def get_proxy() -> str | None:
    """AUTO_DL_PROXY 优先，其次 ALL_PROXY / HTTPS_PROXY / HTTP_PROXY。"""
    load_dotenv()
    for key in (
        "AUTO_DL_PROXY",
        "ALL_PROXY",
        "all_proxy",
        "HTTPS_PROXY",
        "https_proxy",
        "HTTP_PROXY",
        "http_proxy",
    ):
        value = os.environ.get(key, "").strip()
        if value:
            return value
    return None


def requests_proxies() -> dict[str, str]:
    proxy = get_proxy()
    if not proxy:
        return {}
    return {"http": proxy, "https": proxy}


def ensure_library_proxy() -> None:
    """让只认环境变量的库（httpx）走同一条代理。不覆盖已有值。"""
    proxy = get_proxy()
    if not proxy:
        return
    if proxy.startswith("socks5h://"):
        proxy = "socks5://" + proxy[len("socks5h://") :]
    for key in (
        "ALL_PROXY",
        "all_proxy",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "http_proxy",
        "https_proxy",
    ):
        os.environ.setdefault(key, proxy)
