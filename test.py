import requests
url = 'https://www.mediafire.com/file/qjceui94toh5i8s/@eaglecloudofficial+Fresh+Ulp.txt/file'

resp = requests.get(url)

with open('./content.html','w',encoding='utf-8') as f:
    f.write(resp.text)