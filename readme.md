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

1. **粘贴 URL** — 每行一个，自动去重
2. **设置保存目录**（可选，默认 `./downloads`）
3. 点击 **提交 URL** — 自动识别可下载和不支持的链接
4. 点击 **开始下载** — 5 个线程并发下载
5. 在任务列表中查看实时进度、速度和文件大小

## 配置

| 环境变量 | 默认值 | 说明 |
|---------|--------|------|
| `AUTO_DL_WORKERS` | `5` | 并发下载线程数 |
| `GOFILE_DL_BASE_URL` | `http://192.168.3.160:2355` | gofile-dl 服务地址 |
| `GOFILE_DL_USERNAME` / `GOFILE_DL_PASSWORD` | `admin` / `Abc123!!` | gofile-dl 的 Basic Auth |
| `GOFILE_DL_HOST_DIR` | `/opt/gofile-dl/downloads` | gofile-dl 容器 `/data` 映射到的宿主机目录 |
| `GOFILE_DL_REMOTE_DIR` | `/data` | gofile-dl 容器内下载目录（勿改，除非卷映射不同） |

各下载器支持标准代理环境变量（`ALL_PROXY`、`HTTPS_PROXY`）来走代理。

> **gofile.io 已改为转发模式**：不再直连 gofile.io API，而是把链接提交给本机跑的
> gofile-dl 容器（见 `test/docker-compose.yml`）下载，完成后把结果从
> `GOFILE_DL_HOST_DIR` 拍平复制回你在前端指定的输出目录。

## 功能特性

- **并发下载** — 可配置线程池
- **断点续传** — 中断后自动续传
- **URL 去重** — 相同链接自动跳过
- **实时进度** — SSE 推送状态更新到浏览器
- **失败重试** — 支持单个或全部重试
- **深色主题** — 响应式布局，支持移动端

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
