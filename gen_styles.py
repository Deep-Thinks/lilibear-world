"""
栗栗熊 5风格 × 3城市 批量生成脚本。

调用本地 lilibear server.py 反代 (localhost:18080) → 上游 gpt-image-2。
输出到 /HUDONGE/pilot/lilibear_styles/<city>/<style>.png （20027 端口可见）。

设计要点（根据 GPT Image 2 prompt best practice 提炼）:
  1. 结构化六要素: TASK / SUBJECT / STRUCTURE / MEDIUM-PALETTE-LINE / TEXT / BOUNDARY
  2. Identity lock: 锁定小熊猫的脸型、毛色、尾巴环纹（防止画成浣熊或别的熊）
  3. Preserve / Change-only / Avoid 三段式适配参考图改写场景
  4. 中文输出限制为短菜名标签（≤ 6 字），不写长段中文正文
  5. 5 风格各自独立 visual-system，但共享 LAYOUT 与 IDENTITY 不变量
"""
import io
import os
import sys
import time
import uuid
import json
import urllib.request
import urllib.error
import mimetypes
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

PROXY_URL = "http://localhost:18080/api/dmfox/gptapi/v1/images/edits"
REF_DIR = "/niuniu869_dev/lilibear_world/refs_compressed"
OUT_DIR = "/HUDONGE/pilot/lilibear_styles"
MANIFEST_PATH = os.path.join(OUT_DIR, "manifest.json")
LOG_PATH = os.path.join(OUT_DIR, "gen.log")
SIZE = "1024x3072"
MODEL = "gpt-image-2"
N = 1
MAX_PARALLEL = 1      # 实测上游 dm-fox 并发 ≥ 2 立刻 429，严格串行最稳
TIMEOUT_SEC = 600
RATE_LIMIT_BACKOFF = 75   # 429 时退避秒数（避免聚簇）
NORMAL_BACKOFF = 12       # 其它错误退避秒数
MAX_ATTEMPTS = 6          # 单张最多 6 次尝试

# —— 共享 identity / layout 约束 —— 写英文（结构指令更稳）
IDENTITY = (
    "Identity lock (use provided reference images as canonical mascot): "
    "the protagonist is LiliBear — a SMALL RED PANDA (not a raccoon, not a brown bear), "
    "rust-orange fur, cream face with dark eye masks, rounded ears with white tufts, "
    "long fluffy tail with alternating reddish-brown and cream rings, plump rounded body, "
    "innocent gentle expression. Keep face shape, fur color palette, ear shape and tail "
    "ring pattern identical across all panels."
)

LAYOUT = (
    "Canvas: 1024x3072 vertical poster, aspect ratio 1:3. "
    "Compose 5 to 7 stacked food modules from top to bottom, evenly spaced, "
    "with a clear visual rhythm. Each module contains: (a) one signature local dish "
    "rendered with appetizing detail, (b) ONE short Simplified-Chinese dish-name label "
    "of at most 6 characters next to it, (c) LiliBear in a distinct action — eating, "
    "hugging the dish, drooling, holding it up, lying belly-up, or peeking from behind. "
    "Top of the poster shows the city name in a stylised header band. Bottom leaves a "
    "small footer area. No long paragraphs anywhere."
)

TEXT_RULE = (
    "Text output rules: only short Simplified-Chinese dish names (each label ≤ 6 chinese "
    "characters), plus the city name header. No English captions inside food modules, "
    "no fake watermarks, no signatures, no QR codes, no garbled characters."
)

BOUNDARY = (
    "Hard avoid: photorealistic photography, 3D CGI render, anime moe-girl style, "
    "human characters of any kind (only LiliBear is allowed as a character), multiple "
    "mascots fighting for focus, inconsistent fur color between panels, long sentences, "
    "logos of real restaurant brands."
)

# —— 5 风格 visual-system —— 区分点：medium / palette / line / mood / extras
STYLES = {
    "01_warm_watercolor": {
        "label_zh": "温润手绘水彩（原版升级）",
        "medium": "Traditional watercolor on warm cream paper. Wet-on-wet pigment blooms, "
                  "soft edges, light ink outlines drawn with a small round brush. Visible "
                  "paper grain. Slight pencil under-sketch peeking through.",
        "palette": "Warm earthy palette: cream paper #FBF3E4 background, terracotta red, "
                   "sage green, dusty rose, soft mustard, ink brown #4A342A for outlines.",
        "line": "Hand-drawn ink contour, intentionally wobbly, with occasional dry-brush tips.",
        "mood": "Cozy slow-Sunday-afternoon food-diary feeling, like a hand-bound travel sketchbook.",
        "extras": "Add tiny watercolor splashes around dishes. City header in playful brush "
                  "calligraphy. Footer in a soft dashed line.",
    },
    "02_showa_diner": {
        "label_zh": "昭和食堂复古海报",
        "medium": "Gouache illustration in 1970s Japanese shokudo magazine style. "
                  "Flat opaque color fills with very subtle paper grain. Strong uniform "
                  "black contours, simple cel-shading.",
        "palette": "Retro warm palette: mustard yellow #E5B741, vermilion red #D04B2C, "
                   "indigo navy #2C3E66, off-white #F4ECDC, ink black #1A1815. "
                   "Limit to these 5 colors only.",
        "line": "Bold uniform 2-3 pt jet-black contour lines around every shape; "
                "no sketchy strokes; consistent stroke weight.",
        "mood": "Nostalgic neighborhood eatery poster, warm-bellied, slightly cheeky.",
        "extras": "City header set in a horizontal banner with retro san-serif Chinese "
                  "type, faux-print registration offset for halftone charm. Small starburst "
                  "shapes near key dishes.",
    },
    "03_rice_paper_inkwash": {
        "label_zh": "中式宣纸水墨小写意",
        "medium": "Sumi-e ink wash with light color on aged rice paper. Dry-wet brush "
                  "variation, expressive single-stroke shapes, deliberate negative space.",
        "palette": "Minimal palette: deep ink black, raw umber, washed vermilion (used "
                   "ONLY for a small seal), bamboo green, faded paper beige. Most surface "
                   "left as paper.",
        "line": "Calligraphic brush strokes with dramatic dry-to-wet contrast; outlines "
                "vary in thickness; some shapes are pure ink silhouette.",
        "mood": "Literati food album, lyrical and quiet, a small-essay-with-paintings feel.",
        "extras": "Top of poster carries the city name in vertical brush calligraphy + "
                  "one small red square seal (碧落 stamp aesthetic). Subtle bamboo leaves "
                  "as connectors between modules.",
    },
    "04_vintage_postcard": {
        "label_zh": "复古旅行明信片",
        "medium": "1950s screen-printed travel poster aesthetic. Flat color shapes with "
                  "visible halftone dot grain and slight CMYK registration offset.",
        "palette": "Faded postcard palette: cream #F2E6C8, faded turquoise #5BA8A0, "
                   "coral red #D86452, mustard #D6A744, sepia ink #4A3624.",
        "line": "Flat poster illustration; outlines only where needed to separate color "
                "fields; smooth simplified shapes.",
        "mood": "Souvenir 'Greetings from [city]' postcard, nostalgic but cheerful.",
        "extras": "Top section framed as a postage stamp with serrated edge and the city "
                  "name in vintage display type. Bottom edge with a thin dashed cut line "
                  "and a small 'PAR AVION' style mark. Air-mail blue-red striped border "
                  "running down the side margins.",
    },
    "05_flat_pastel_modern": {
        "label_zh": "极简扁平甜系",
        "medium": "Modern flat vector illustration with soft pastel gradients, rounded "
                  "geometric shapes, very subtle drop shadows; clean and bright.",
        "palette": "Soft pastel palette: peach #FFD9C2, mint #C8E6D0, butter #FFE9B3, "
                   "lilac #D9CCEF, paper #FFFAF3, with a darker accent #5A4A42 for tiny "
                   "details only.",
        "line": "No heavy outlines; if needed, a 1-pt soft outline; rely on color fields "
                "and gentle shadows to define shapes.",
        "mood": "Modern, instagrammable, contemporary café-menu illustration, friendly.",
        "extras": "City name set in a rounded sans-serif chip at the top. Small floating "
                  "abstract shapes (dots, soft squiggles) as background rhythm. Generous "
                  "negative space between modules.",
    },
}

# —— 城市配置 —— 每城显式给出 5–7 道代表菜（避免模型乱编）
CITIES = {
    "xiamen": {
        "name_zh": "厦门",
        "name_en": "Xiamen",
        "coord": "(24.48°N, 118.10°E)",
        "dishes": [
            "沙茶面 (peanut-sesame satay noodles, the city's signature)",
            "海蛎煎 (oyster omelette with sweet chili sauce)",
            "土笋冻 (jellied sea-worm in clear aspic, a coastal curiosity)",
            "烧肉粽 (rich braised-pork zongzi, served with sweet sauce)",
            "花生汤配油条 (sweet peanut soup with a fried dough stick)",
            "厦门薄饼 (springroll-style popiah, multi-veggie filling)",
            "姜母鸭 (clay-pot ginger-mother duck)",
        ],
        "scene_hint": "Subtropical coastal vibe — phoenix-flower trees, granite alleys, "
                      "minnan red-roof tiles, gentle sea breeze.",
    },
    "hongkong": {
        "name_zh": "香港",
        "name_en": "Hong Kong",
        "coord": "(22.30°N, 114.17°E)",
        "dishes": [
            "菠萝油 (pineapple bun with thick cold butter slab)",
            "丝袜奶茶 (silk-stocking milk tea, deep amber color)",
            "烧鹅濑粉 (roast goose over rice noodles in clear broth)",
            "云吞面 (shrimp wonton noodles with thin egg noodles)",
            "蛋挞 (golden egg tart, flaky crust)",
            "鱼蛋串 (curry fish-ball street skewer)",
            "煲仔饭 (clay-pot rice with crispy bottom, sweet soy)",
        ],
        "scene_hint": "Dense neon-sign cha-chaan-teng energy, double-decker tram red, "
                      "tropic harbor humidity, cantonese signage rhythm.",
    },
    "newyork": {
        "name_zh": "纽约",
        "name_en": "New York",
        "coord": "(40.71°N, 74.01°W)",
        "dishes": [
            "NY 披萨切片 (giant foldable pepperoni slice, grease drip)",
            "贝果三文鱼 (everything bagel with lox and cream cheese)",
            "熏牛肉三明治 (towering pastrami on rye with mustard)",
            "纽约热狗 (street-cart hot dog with sauerkraut and mustard)",
            "纽约芝士蛋糕 (dense plain cheesecake slice)",
            "盐结饼 (soft pretzel from a street cart)",
            "盖浇鸡饭 (halal chicken-and-rice with white sauce)",
        ],
        "scene_hint": "Yellow-cab and steam-grate Manhattan grit, deli signage, "
                      "subway-tile diner walls, brick walk-ups.",
    },
}


def build_prompt(city_key, style_key):
    """组装最终 prompt。结构化六要素 + 三段式（preserve/change/avoid）。"""
    city = CITIES[city_key]
    style = STYLES[style_key]
    dishes_block = "\n".join("  - " + d for d in city["dishes"])

    parts = [
        "TASK: Create an illustrated vertical food-map poster of " + city["name_en"] +
        " (Chinese: " + city["name_zh"] + ", " + city["coord"] + "), starring LiliBear "
        "the Xiamen-University-Food-Society mascot.",

        "SUBJECT (identity lock — strictly preserve from reference images):\n" + IDENTITY,

        "STRUCTURE / LAYOUT:\n" + LAYOUT,

        "SCENE & DISH PALETTE (you MUST pick 5-7 of the following, do NOT invent unrelated "
        "dishes; render each one recognizably):\n" + dishes_block +
        "\nAmbient scene hint: " + city["scene_hint"],

        "VISUAL SYSTEM (style — this is the variable that differs across this batch):"
        "\n  Medium: " + style["medium"] +
        "\n  Palette: " + style["palette"] +
        "\n  Line: " + style["line"] +
        "\n  Mood: " + style["mood"] +
        "\n  Extras: " + style["extras"],

        "TEXT RULES:\n" + TEXT_RULE,

        "PRESERVE FROM REFERENCE: LiliBear's species, fur color zones, ear shape, "
        "tail ring pattern, plump body proportions, warm naive expression.",

        "CHANGE FROM REFERENCE: the rendering medium, palette, environment, dishes shown, "
        "composition layout (now a tall 1:3 vertical food poster).",

        "AVOID:\n" + BOUNDARY,

        "Final note: keep visual rhythm consistent top-to-bottom; do NOT crowd the canvas; "
        "leave 4-6% breathing space at top and bottom edges.",
    ]
    return "\n\n".join(parts)


# ---------- multipart 手搓 ----------
def _mp_body(fields, files):
    boundary = "----lilibear" + uuid.uuid4().hex
    body = io.BytesIO()
    for k, v in fields:
        body.write(("--" + boundary + "\r\n").encode())
        body.write(('Content-Disposition: form-data; name="' + k + '"\r\n\r\n').encode())
        body.write(v.encode("utf-8"))
        body.write(b"\r\n")
    for k, fpath in files:
        fn = os.path.basename(fpath)
        ctype = mimetypes.guess_type(fn)[0] or "application/octet-stream"
        body.write(("--" + boundary + "\r\n").encode())
        body.write(('Content-Disposition: form-data; name="' + k +
                    '"; filename="' + fn + '"\r\n').encode())
        body.write(("Content-Type: " + ctype + "\r\n\r\n").encode())
        with open(fpath, "rb") as f:
            body.write(f.read())
        body.write(b"\r\n")
    body.write(("--" + boundary + "--\r\n").encode())
    return body.getvalue(), boundary


def _log(msg):
    line = "[%s] %s" % (time.strftime("%H:%M:%S"), msg)
    print(line, flush=True)
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def _load_refs():
    refs = []
    for fn in sorted(os.listdir(REF_DIR)):
        if fn.lower().endswith((".jpg", ".jpeg", ".png", ".webp")):
            refs.append(os.path.join(REF_DIR, fn))
    return refs


def call_api(prompt, refs):
    fields = [("model", MODEL), ("prompt", prompt), ("size", SIZE), ("n", str(N))]
    files = [("image", p) for p in refs]
    body, boundary = _mp_body(fields, files)
    req = urllib.request.Request(
        PROXY_URL,
        data=body,
        method="POST",
        headers={"Content-Type": "multipart/form-data; boundary=" + boundary,
                 "Content-Length": str(len(body))},
    )
    with urllib.request.urlopen(req, timeout=TIMEOUT_SEC) as resp:
        raw = resp.read()
        return json.loads(raw.decode("utf-8"))


def save_image(b64_or_url, out_path):
    import base64
    if b64_or_url.startswith("http"):
        with urllib.request.urlopen(b64_or_url, timeout=120) as r:
            data = r.read()
    else:
        data = base64.b64decode(b64_or_url)
    with open(out_path, "wb") as f:
        f.write(data)
    return len(data)


def gen_one(city_key, style_key, refs, manifest, lock):
    out_dir = os.path.join(OUT_DIR, city_key)
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, style_key + ".png")
    if os.path.exists(out_path) and os.path.getsize(out_path) > 50000:
        _log("[skip] " + city_key + "/" + style_key + " 已存在")
        return True
    prompt = build_prompt(city_key, style_key)
    t0 = time.time()
    _log("[start] " + city_key + " × " + style_key)
    last_err = None
    import random
    for attempt in range(MAX_ATTEMPTS):
        try:
            j = call_api(prompt, refs)
            item = (j.get("data") or [{}])[0]
            payload = item.get("b64_json") or item.get("url")
            if not payload:
                raise RuntimeError("no image payload: " + json.dumps(j)[:300])
            bytes_n = save_image(payload, out_path)
            dur = time.time() - t0
            _log("[ok]   " + city_key + " × " + style_key +
                 " → %.1fKB in %.1fs" % (bytes_n / 1024, dur))
            with lock:
                manifest.setdefault(city_key, {})[style_key] = {
                    "path": os.path.relpath(out_path, OUT_DIR),
                    "bytes": bytes_n,
                    "duration_sec": round(dur, 1),
                    "attempts": attempt + 1,
                }
                with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
                    json.dump(manifest, f, ensure_ascii=False, indent=2)
            return True
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", "replace")[:300] if e.fp else ""
            last_err = "HTTP " + str(e.code) + " " + body
            is_429 = (e.code == 429) or ("rate_limit" in body) or ("并发请求" in body)
            backoff = RATE_LIMIT_BACKOFF if is_429 else NORMAL_BACKOFF
            backoff += random.uniform(0, 8)
            _log("[retry " + str(attempt + 1) + "/" + str(MAX_ATTEMPTS) + "] " +
                 city_key + " × " + style_key + " ← " + last_err[:140] +
                 " (sleep %.0fs)" % backoff)
            time.sleep(backoff)
        except Exception as e:
            last_err = type(e).__name__ + ": " + str(e)
            backoff = NORMAL_BACKOFF + random.uniform(0, 6)
            _log("[retry " + str(attempt + 1) + "/" + str(MAX_ATTEMPTS) + "] " +
                 city_key + " × " + style_key + " ← " + last_err[:140] +
                 " (sleep %.0fs)" % backoff)
            time.sleep(backoff)
    _log("[FAIL] " + city_key + " × " + style_key + " ← " + (last_err or "?"))
    with lock:
        manifest.setdefault(city_key, {})[style_key] = {"error": last_err}
        with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)
    return False


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    refs = _load_refs()
    _log("refs: " + str(len(refs)) + " files, " +
         ", ".join(os.path.basename(r) for r in refs))
    manifest = {}
    if os.path.exists(MANIFEST_PATH):
        with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
            try:
                manifest = json.load(f)
            except Exception:
                manifest = {}
    lock = threading.Lock()
    # 任务顺序: 同一风格跨城连排，方便用户半路看也能跨城对比同风格
    jobs = [(c, s) for s in STYLES for c in CITIES]
    _log("queued %d jobs, parallel=%d" % (len(jobs), MAX_PARALLEL))
    with ThreadPoolExecutor(max_workers=MAX_PARALLEL) as ex:
        futs = {ex.submit(gen_one, c, s, refs, manifest, lock): (c, s)
                for (c, s) in jobs}
        for fut in as_completed(futs):
            c, s = futs[fut]
            try:
                fut.result()
            except Exception as e:
                _log("[job-FAIL] " + c + "/" + s + " ← " + str(e))
    _log("=== done ===")
    # 统计
    ok = 0
    fail = 0
    for c in CITIES:
        for s in STYLES:
            entry = manifest.get(c, {}).get(s)
            if entry and "path" in entry:
                ok += 1
            else:
                fail += 1
    _log("summary: ok=%d, fail=%d" % (ok, fail))


if __name__ == "__main__":
    main()
