# 自动化下载工具

批量下载工具，提供 Web 界面。粘贴文件分享链接，自动识别网站并并发下载，支持断点续传和实时进度展示。

## 支持的网站

| 网站 | URL 格式 |
|------|---------|
| [gofile.io](https://gofile.io) | `gofile.io/d/...` |
| [upload.ee](https://upload.ee) | `upload.ee/files/...` |
| [pixeldrain.com](https://pixeldrain.com) | `pixeldrain.com/u/...` / `pixeldrain.com/l/...` |
| [mediafire.com](https://mediafire.com) | `mediafire.com/file/...` |
| [transfer.it](https://transfer.it) | `transfer.it/t/...` |
| [anonfilesnew.com](https://anonfilesnew.com) | `anonfilesnew.com/...` |
| [biteblob.com](https://biteblob.com) | `biteblob.com/...` |

## 快速开始

```bash
# 安装依赖
pip install -r requirements.txt

# 启动服务
python app.py

# 自定义端口
python app.py --host 0.0.0.0 --port 8080
```

浏览器打开 `http://localhost:5000`。

## 使用流程

1. **粘贴文字** — 程序从中找出链接，空行和重复的会跳过
2. **可选填子目录** — 文件默认落在 `downloads/当天日期/`，不能填写服务器上的任意路径
3. 点击 **提交并开始** — 这一批马上下载。线程数由 `AUTO_DL_WORKERS` 决定
4. **停止、继续、重试、清空** 只作用于选中的那一批
5. 完成后可以在页面里搜索文件，并下载到自己的电脑。失败原因可以复制
6. 服务重启后，未完成和失败的任务还在，需要点 **继续本批** 才会接着下

## 配置

配置放在项目根目录的 `.env`（参考 `.env.example`，不要提交）。进程里已经设置的环境变量优先于 `.env`。

| 环境变量 | 默认值 | 说明 |
|---------|--------|------|
| `AUTO_DL_PROXY` | 空 | 各下载器共用的代理。未设置时依次看 `ALL_PROXY`、`HTTPS_PROXY`、`HTTP_PROXY` |
| `AUTO_DL_WORKERS` | `5` | 并发下载线程数 |
| `GOFILE_DL_BASE_URL` | `http://192.168.3.160:2355` | gofile-dl 服务地址 |
| `GOFILE_DL_USERNAME` / `GOFILE_DL_PASSWORD` | 空 | gofile-dl 的 Basic Auth，必须设置 |
| `GOFILE_DL_HOST_DIR` | `/opt/gofile-dl/downloads` | gofile-dl 容器 `/data` 映射到的宿主机目录 |
| `GOFILE_DL_REMOTE_DIR` | `/data` | gofile-dl 容器内下载目录（勿改，除非卷映射不同） |

本机和服务器用各自的 `.env`。gofile 要把结果从 `GOFILE_DL_HOST_DIR` 拷回输出目录，所以跑 gofile 的进程必须和那个目录在同一台机器上。

> **gofile.io 已改为转发模式**：不再直连 gofile.io API，而是把链接提交给本机跑的
> gofile-dl 容器（见 `test/docker-compose.yml`）下载，完成后把结果从
> `GOFILE_DL_HOST_DIR` 拍平复制回当天的 `downloads` 目录。

## 功能特性

- **并发下载** — 可配置线程池
- **按批次操作** — 停止和清空不会影响其他人的任务
- **任务可恢复** — 未完成和失败的任务记在 `data/state.json`（不提交）
- **URL 去重** — 相同链接自动跳过
- **实时进度** — SSE 推送状态更新到浏览器
- **失败重试** — 可以重试单个任务，或重试一批里的失败任务
- **页面取文件** — 已完成的文件可以直接下载到浏览器
- **深色主题** — 宽屏用表格，窄屏用卡片

## 项目结构

```
auto_download/
├── app.py                  # Flask Web 服务 + 任务管理
├── requirements.txt        # Python 依赖
├── templates/
│   └── index.html          # Web 界面（单页应用）
├── utils/
│   ├── gofile_downloader.py
│   ├── uploadee_downloader.py
│   ├── pixeldrain_downloader.py
│   ├── mediafire_downloader.py
│   ├── transferit_downloader.py
│   ├── anonfilesnew_downloader.py
│   └── biteblob_downloader.py
└── downloads/              # 默认下载目录
```

## API 端点

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/submit` | 提交 URL 列表 |
| POST | `/api/start` | 开始/继续下载 |
| POST | `/api/stop` | 停止所有下载 |
| POST | `/api/retry` | 重试失败任务（可选 `{"index": N}`） |
| POST | `/api/clear` | 清空已完成/失败的任务 |
| GET | `/api/status` | 获取完整状态（JSON） |
| GET | `/api/stream` | SSE 实时状态推送 |

## License

MIT
