import requests
import json
import subprocess

url = "https://cloud.mail.ru/public/N1uo/1LAu8zNcY"
proxy = {"http": "127.0.0.1:7897", "https": "127.0.0.1:7897"}
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36 Edg/150.0.0.0",
}

resp = requests.get(url, headers=headers, proxies=proxy)
html = resp.text

# cloudSettings 是 JS 对象字面量（含字符串拼接、\xNN 转义），不是纯 JSON
# 用正则 + json.loads 搞不定，交给 Node.js 去 eval 再导出 JSON
node_script = r"""
let html = '';
process.stdin.setEncoding('utf8');
process.stdin.on('data', chunk => html += chunk);
process.stdin.on('end', () => {
    const idx = html.indexOf('window.cloudSettings');
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

result = subprocess.run(
    ["node", "-e", node_script],
    input=html,
    capture_output=True,
    text=True,
    timeout=30,
    encoding="utf-8",
    errors="replace",
)

if result.returncode != 0:
    print(f"Node failed: {result.stderr}")
    exit(1)

data = json.loads(result.stdout.strip())
file_list = data.get("params", {}).get("serverSideFolders", {}).get("list", [])
weblink = ""
name = ""
# post_body = {"x-email": "anonym", "weblink_list": ["N1uo/1LAu8zNcY/APB LogPass Base By Chucky [MAILPASS.PW]"], "name": "APB LogPass Base By Chucky [MAILPASS.PW]"}
post_body = {"x-email": "anonym", "weblink_list": [weblink], "name": name}
url = "https://cloud.mail.ru/api/v3/zip/weblink"
for item in file_list:
    name = item.get("name")
    weblink = item.get("weblink")
    post_body["name"] = name
    post_body["weblink_list"][0] = weblink

    print(name, weblink)
    print(post_body)
    resp = requests.post(url, headers=headers, json=post_body, proxies=proxy)
    print(resp.status_code, resp.text)
    download_link = resp.json().get("key", {})
    if download_link:
        print("Download link:", download_link)
    print("===" * 20)

# with open("cloud_mail_ru.json", "w", encoding="utf-8") as f:
#     json.dump(data, f, ensure_ascii=False, indent=2)

# print("Done — cloud_mail_ru.json saved")
