"""探测 dm-fox 的多图 + 非标准 size 支持。
策略：尽量短 prompt + n=1 节省成本，重点看 HTTP 状态码 + 错误消息体。
"""
import requests, time, sys, json, os, glob

API_KEY = 'sk-ant-oat01-pZnkO4eP6rGmOfLcQTIPkwOYcGq9ijlITK2hv77hNYqX3NB-HgafBI4Ywxml4CtyJSJxGDbtBkMRKLdJ7__lBKCwgq68MAA'
EDIT_URL = 'https://dm-fox.rjj.cc/gptapi/v1/images/edits'
GEN_URL = 'https://dm-fox.rjj.cc/gptapi/v1/images/generations'

REFS = sorted(glob.glob('refs_compressed/*.jpg'))
PROMPT = 'a tiny cute red panda mascot in this style, watercolor'

def post_edit(images, size, n=1, prompt=PROMPT, field='image'):
    files = []
    for i, p in enumerate(images):
        # OpenAI 标准：重复传 'image' 字段
        files.append((field, (f'ref{i}.jpg', open(p, 'rb'), 'image/jpeg')))
    data = {'model': 'gpt-image-2', 'prompt': prompt, 'size': size, 'n': str(n)}
    t0 = time.time()
    try:
        r = requests.post(EDIT_URL, headers={'Authorization': f'Bearer {API_KEY}'},
                          files=files, data=data, timeout=240)
        dt = time.time() - t0
        return r.status_code, r.text[:500], dt
    except Exception as e:
        return -1, str(e), time.time() - t0
    finally:
        for _, (_, fh, _) in files:
            fh.close()

def probe(name, **kw):
    print(f'\n=== {name} ===')
    code, body, dt = post_edit(**kw)
    print(f'HTTP {code} ({dt:.1f}s)')
    if code == 200:
        try:
            j = json.loads(body)
            url = j.get('data',[{}])[0].get('url') or j.get('data',[{}])[0].get('b64_json','')[:60]
            print(f'OK url={url[:120]}')
        except Exception:
            print(body[:200])
    else:
        print(body)

if __name__ == '__main__':
    test = sys.argv[1] if len(sys.argv) > 1 else 'all'
    if test in ('all', '1'):
        probe('T1: baseline 1 image, 1024x1024', images=REFS[:1], size='1024x1024')
    if test in ('all', '2'):
        probe('T2: 6 images, 1024x1024', images=REFS, size='1024x1024')
    if test in ('all', '3'):
        probe('T3: 1 image, 1024x3072 (1:3)', images=REFS[:1], size='1024x3072')
    if test in ('all', '4'):
        probe('T4: 1 image, 1024x5120 (1:5)', images=REFS[:1], size='1024x5120')
