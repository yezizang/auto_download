"""测试 biteblob 下载器。"""
from utils.biteblob_downloader import download

if __name__ == "__main__":
    url = "https://biteblob.com/Information/fsWfXr8bNiWrZ3/#Secretline.top"
    result = download(url, "./downloads")
    if result:
        print("OK:", result)
    else:
        print("FAILED")
