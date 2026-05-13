"""端到端测试：模拟前端通过本地代理调用 dm-fox edits。
验证：6 张参考图 + 1024x3072 + 完整中文 prompt 能正确拿到图片。
"""
import requests, time, base64, json, glob, sys

API_KEY = 'sk-ant-oat01-pZnkO4eP6rGmOfLcQTIPkwOYcGq9ijlITK2hv77hNYqX3NB-HgafBI4Ywxml4CtyJSJxGDbtBkMRKLdJ7__lBKCwgq68MAA'
URL = 'http://localhost:18080/api/dmfox/gptapi/v1/images/edits'
REFS = sorted(glob.glob('refs_compressed/*.jpg'))

# 模拟前端 buildPrompt(35.01, 135.77)（京都）
lat, lng = 35.01, 135.77
loc_str = f'({abs(lat):.2f}°N, {abs(lng):.2f}°E)'
PROMPT = (
    '这是我们厦门大学美食协会的吉祥物栗栗熊的原始设计图（是一只小熊猫，不是小浣熊）。'
    '\n\n请为我根据这个设计图和画风，制作出栗栗熊在经纬度 ' + loc_str +
    ' 这个位置的美食地图。'
    '\n\n请生成一张超长的竖版长图，从上到下排布当地最具代表性的菜肴/小吃/食材/餐桌场景，'
    '栗栗熊以不同动作（吃、抱、流口水、举着、躺平）穿插其中，并标注食物名字。'
    '\n\n如果该经纬度落在海洋上，请改画就近最有名的沿海港口/海岛的代表性海鲜与海味。'
    '画风保持原设计图的温暖手绘水彩、米白底色、可爱粗线条。'
)

files = [('image', (f'ref{i}.jpg', open(p, 'rb'), 'image/jpeg')) for i, p in enumerate(REFS)]
data = {'model': 'gpt-image-2', 'prompt': PROMPT, 'size': '1024x3072', 'n': '1'}

print(f'Posting to {URL}')
print(f'Refs: {len(REFS)}, size: 1024x3072, prompt len: {len(PROMPT)}')
t0 = time.time()
try:
    r = requests.post(URL, headers={'Authorization': f'Bearer {API_KEY}'},
                      files=files, data=data, timeout=300)
finally:
    for _, (_, fh, _) in files:
        fh.close()
dt = time.time() - t0
print(f'HTTP {r.status_code} ({dt:.1f}s) body-bytes={len(r.content)}')
print('Content-Type:', r.headers.get('Content-Type'))
print('Content-Encoding:', r.headers.get('Content-Encoding'))
print('first 200 bytes (raw):', r.content[:200])
print('last 100 bytes:', r.content[-100:])
# save raw body for inspection
with open('e2e_raw_body.bin', 'wb') as f:
    f.write(r.content)
print('raw body saved to e2e_raw_body.bin')
if r.status_code != 200:
    print('FAIL:', r.text[:600])
    sys.exit(1)
j = r.json()
item = j['data'][0]
if 'b64_json' in item:
    out = 'e2e_output.png'
    with open(out, 'wb') as f:
        f.write(base64.b64decode(item['b64_json']))
    import os
    print(f'OK saved {out} ({os.path.getsize(out)/1024:.0f} KB)')
else:
    print('OK url:', item.get('url'))
