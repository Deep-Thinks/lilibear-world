"""
栗栗熊 v2: 一日时辰蜿蜒轴 layout × 3 城市 批量生成脚本。

v2 已选定唯一方案: v2_01_day_trail (一日时辰蜿蜒轴) —— 单熊从晨走到夜，
朱砂蜿蜒小径串 6–7 个时辰停靠站。删除 v2_02 ~ v2_05 四个试错版式。

设计契约（与生产端 index.html:buildPrompt 严格对齐）:
  生产端唯一变量 = location label（含中英文地名 + 经纬度坐标）。
  菜单 / 场景氛围 / 海洋兜底 全部由模型基于 location 自己推断，
  prompt 模板里不预填任何"该城市有什么菜"的清单。
  因此 CITIES["dishes"] / CITIES["scene_hint"] 在 v2 不再注入 prompt，
  仅作为 v1 历史包袱保留。

==========================================================================
SYNC CONTRACT —— Python 端 build_prompt_for_location() 与
JS 端 index.html:buildPrompt() 必须保持字面一致。任何一处改动后:
  1. 同步另一端的常量块；
  2. 跑 dry-run 对比两端输出的 prompt 文本应字符级相同。
==========================================================================

跑法:
  python3 gen_styles_v2.py
输出: /HUDONGE/pilot/lilibear_styles_v2/<city>/v2_01_day_trail.png
"""
import os
import sys
import time
import json
import threading
import random
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed

import gen_styles as v1  # 复用 CITIES / IDENTITY / TEXT_RULE / BOUNDARY / call_api / save_image

OUT_DIR = "/HUDONGE/pilot/lilibear_styles_v2"
MANIFEST_PATH = os.path.join(OUT_DIR, "manifest.json")
LOG_PATH = os.path.join(OUT_DIR, "gen.log")
MAX_PARALLEL = 1
MAX_ATTEMPTS = v1.MAX_ATTEMPTS
RATE_LIMIT_BACKOFF = v1.RATE_LIMIT_BACKOFF
NORMAL_BACKOFF = v1.NORMAL_BACKOFF

STYLE_KEY = "v2_01_day_trail"
STYLE_LABEL_ZH = "一日时辰蜿蜒轴"


# ===== Prompt 常量块 (与 index.html:buildPrompt 字面一致, 修改请同步) =====

# IDENTITY / TEXT_RULES / BOUNDARY 直接复用 v1 的字符串字面
# (它们的内容已在 gen_styles.py 定稿, 不在此重复定义)
IDENTITY = v1.IDENTITY
TEXT_RULES = v1.TEXT_RULE
BOUNDARY = v1.BOUNDARY

LAYOUT_SKELETON = (
    "LAYOUT SKELETON = a single hand-drawn meandering vermilion trail that snakes "
    "from the top of the canvas down to the bottom like a river or a Chinese paper-cut "
    "path, dividing the 1024x3072 vertical canvas into 6-7 'stop stations' along its "
    "length. The trail bends left-and-right alternately and NEVER becomes a straight "
    "column. At each station place ONE locally-authentic signature dish for this place, "
    "rendered with appetizing watercolor detail, beside a small ink-stamp showing the "
    "hour of day (eg 卯时 5:00, 辰时 8:00, 午时 12:00, 申时 16:00, 戌时 20:00, "
    "亥时 22:00) and a short Simplified-Chinese dish-name label (≤6 characters) in "
    "handwritten brush pen. LiliBear appears EXACTLY ONCE per station as the SAME "
    "single traveler — at dawn yawning on a folded bicycle, mid-morning sniffing steam "
    "from a bowl, noon sitting cross-legged eating, afternoon balancing a snack on the "
    "head, evening clinking a cup, night belly-up napping — with tiny watercolor "
    "footprints / wheel tracks / dotted lines connecting consecutive stations along "
    "the trail. Top header: a circular postage-stamp seal containing the location name "
    "(中文 + EN if available) and the coordinates. Bottom footer: a tiny embroidered "
    "cushion with LiliBear curled asleep, signed '晚安 · GOOD NIGHT'. ABSOLUTELY NO "
    "uniform stacked rectangular food modules — the rhythm is a winding one-day "
    "journey, not a list."
)

VISUAL_SYSTEM = (
    "  Medium: Traditional warm watercolor on cream paper, wet-on-wet blooms with "
    "visible paper grain and a faint pencil under-sketch peeking through. The trail "
    "itself is painted in a single vermilion brush stroke that occasionally goes "
    "dry-brush.\n"
    "  Palette: Cream paper #FBF3E4 background, vermilion #C8492C trail accent, sage "
    "green, dusty rose, soft mustard, ink brown #4A342A for outlines and time-stamps.\n"
    "  Line: Wobbly hand-drawn ink contours with dry-brush tips; the trail itself is "
    "one continuous brush gesture.\n"
    "  Mood: A cozy slow-Sunday-afternoon travel sketchbook telling one bear's day in "
    "one place.\n"
    "  Extras: Tiny watercolor splashes near dishes; faint pencil grid behind the paper "
    "for hand-bound notebook feel; one small red square seal near the bottom signature."
)


def build_prompt_for_location(location_label):
    """生产端唯一入口 —— 输入完整 location 字符串(含中英文地名+坐标), 输出完整 prompt。

    location_label 示例:
        "厦门 · 思明区 (24.44°N, 118.10°E)"
        "(40.71°N, 74.01°W)"        # 仅坐标(地名反查失败时)
        "Hong Kong (22.30°N, 114.17°E)"

    生产端契约: 此函数是 Python 端 prompt 的唯一信源, 与 index.html:buildPrompt 字面对齐。
    """
    parts = [
        "TASK: Create an illustrated vertical food-map poster (1024x3072, 1:3) of " +
        location_label + ", starring LiliBear the Xiamen-University-Food-Society mascot.",

        "SUBJECT (identity lock — strictly preserve from reference images):\n" + IDENTITY,

        "STRUCTURE / LAYOUT (unique compositional skeleton — under no circumstances "
        "fall back to a column of equally-spaced stacked rectangular food modules):\n" +
        LAYOUT_SKELETON,

        "MENU & AMBIENT (no predefined data — derive both from the location above):\n"
        "Select 5-7 of THE most iconic, locally-authentic signature dishes / street "
        "snacks / signature drinks of " + location_label + ". Choose only foods that "
        "real locals would name as representative; do NOT invent fusion dishes; do NOT "
        "fall back to globally generic foods unless they are genuinely the local "
        "signature. Each dish must be visually recognizable on first glance.\n"
        "Derive the ambient atmosphere (climate, vegetation, architecture, signage, "
        "color mood) from your knowledge of this exact location — do not generalize.\n"
        "Ocean / lake / wilderness fallback: if the coordinates fall on open water or "
        "uninhabited terrain, automatically substitute with the closest well-known "
        "coastal port / island / city and use ITS signature local cuisine and ambient "
        "instead (do not draw an empty sea).",

        "VISUAL SYSTEM (medium / palette / line / mood — hand-drawn watercolor family):\n" +
        VISUAL_SYSTEM,

        "TEXT RULES:\n" + TEXT_RULES,

        "PRESERVE FROM REFERENCE: LiliBear's species, fur color zones, ear shape, "
        "tail ring pattern, plump body proportions, warm naive expression.",

        "CHANGE FROM REFERENCE: rendering medium per the VISUAL SYSTEM above, the "
        "COMPOSITIONAL SKELETON per the LAYOUT above (this is the most important "
        "change and the whole point of v2), the environment, and which dishes are "
        "shown.",

        "AVOID:\n" + BOUNDARY +
        "\nAdditionally STRICTLY AVOID: a vertical column of equally-spaced rectangular "
        "food modules stacked from top to bottom — this is the v1 default trap; the "
        "LAYOUT above exists specifically to break it.",

        "Final note: keep visual rhythm consistent with the chosen LAYOUT skeleton; do "
        "NOT crowd; leave 4-6% breathing space at top and bottom edges.",
    ]
    return "\n\n".join(parts)


def build_prompt(city_key, style_key=STYLE_KEY):
    """批量端兼容入口 —— 从 CITIES dict 拼装 location_label, 再调 build_prompt_for_location。

    仅复用 CITIES 中的 name_zh / coord 两个字段, 不再注入 dishes / scene_hint。
    """
    assert style_key == STYLE_KEY, "v2 已收敛到唯一方案 " + STYLE_KEY
    city = v1.CITIES[city_key]
    # 与前端 formatLocation 输出格式一致: "厦门 (24.48°N, 118.10°E)"
    location_label = city["name_zh"] + " " + city["coord"]
    return build_prompt_for_location(location_label)


# ---------- 跑批基础设施(复用 v1 的 call_api / save_image) ----------
def _log(msg):
    line = "[%s] %s" % (time.strftime("%H:%M:%S"), msg)
    print(line, flush=True)
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(line + "\n")


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
    for attempt in range(MAX_ATTEMPTS):
        try:
            j = v1.call_api(prompt, refs)
            item = (j.get("data") or [{}])[0]
            payload = item.get("b64_json") or item.get("url")
            if not payload:
                raise RuntimeError("no image payload: " + json.dumps(j)[:300])
            bytes_n = v1.save_image(payload, out_path)
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
    refs = v1._load_refs()
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
    jobs = [(c, STYLE_KEY) for c in v1.CITIES]
    _log("queued %d jobs (1 style × %d cities), parallel=%d" %
         (len(jobs), len(v1.CITIES), MAX_PARALLEL))
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
    ok = sum(1 for c in v1.CITIES
             if "path" in manifest.get(c, {}).get(STYLE_KEY, {}))
    fail = len(v1.CITIES) - ok
    _log("summary: ok=%d, fail=%d" % (ok, fail))


if __name__ == "__main__":
    main()
