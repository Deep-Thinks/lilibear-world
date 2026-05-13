"""
Local HTTP server with reverse proxy to dm-fox.rjj.cc.

设计目标:
  - 浏览器只跟 localhost 通信，彻底绕开 CORS / 上游 OPTIONS 401。
  - 图像 API Key (IMAGE_API_KEY) 仅存在于 .env + server 进程内；
    任何客户端请求里的 Authorization 都会被剥掉，避免前端泄漏。
  - 仅放行白名单上游路径 (/v1/images/edits|generations)，
    防止有人拿到 /api/dmfox/ 通道去打其他接口。
  - 每 IP 滑动窗口限流，防止"群众里的坏人"用别人的 key 跑爆量。
  - 集中事件日志 (logs/events.jsonl) + 图片落盘 (logs/images/YYYYMMDD/)。

Endpoints:
  GET  /<file>                          -> 静态文件
  GET  /  or  /index.html               -> 注入 AMAP_KEY 后的 index.html
  ANY  /api/dmfox/v1/images/<edits|generations>  -> 反代 + 注入 Authorization
  POST /api/log/event                   -> 追加事件到 events.jsonl
  POST /api/log/image                   -> 保存生成结果图到 logs/images/

Run:
  python server.py
"""
import http.server
import socketserver
import urllib.request
import urllib.error
import urllib.parse
import ssl
import os
import sys
import socket
import time
import json
import re
import threading
import uuid
import datetime
import contextlib
import base64
import io
import hashlib
import sqlite3
import socket as _socket
from collections import defaultdict, deque
from http.cookies import SimpleCookie

PORT = 18080
# 监听地址：默认 IPv6 双栈（本机开发不变）；生产可在 systemd 注入 BIND_HOST=127.0.0.1
BIND_HOST = os.environ.get('BIND_HOST', '::')
DOC_ROOT = os.path.dirname(os.path.abspath(__file__))
LOG_ROOT = os.path.join(DOC_ROOT, 'logs')
IMG_LOG_ROOT = os.path.join(LOG_ROOT, 'images')
THUMB_ROOT = os.path.join(LOG_ROOT, 'thumbs')        # 服务端缩略图缓存（feed/markers 用）
JOB_LOG_ROOT = os.path.join(LOG_ROOT, 'jobs')
EVENT_LOG_PATH = os.path.join(LOG_ROOT, 'events.jsonl')

# —— 缩略图参数 ——
# 长边 320px / jpeg 78%：feed 卡片 / markers 散点用，约 30~80KB/张
# vs 原图 ~800KB-3MB，节省 ~95% 带宽。
THUMB_MAX_SIDE = 320
THUMB_QUALITY = 78
try:
    from PIL import Image as _PIL_Image
    _HAS_PIL = True
except Exception:
    _PIL_Image = None
    _HAS_PIL = False
UPSTREAM_TIMEOUT = 600
RETRY_TIMES = 3
RETRY_BACKOFF_BASE = 3.0

# —— 任务系统（用于手机端切走/刷新场景的任务持续化）——
# server 端起后台线程跑 dm-fox 上游调用，前端用短轮询拿状态。
# 任务结果落盘到 logs/images/，metadata 落盘到 logs/jobs/{job_id}.json。
JOB_RETENTION_SEC = 24 * 3600          # 完成态任务保留 24 小时（前端基本 24h 内能拉到）
JOB_GC_INTERVAL_SEC = 600              # GC 周期 10 分钟
JOB_MAX_IN_MEMORY = 500                # 内存上限（防内存爆）

# —— 限流参数 ——
RATE_WINDOW_SEC = 3600       # 代理：滑动窗口 1 小时
RATE_MAX_PER_IP = 30         # 代理：每 IP 每窗口最多 30 次
LOG_RATE_WINDOW_SEC = 60     # 日志：滑动窗口 1 分钟
LOG_RATE_MAX_EVENT = 120     # 日志事件：每 IP 每分钟 120 条
LOG_RATE_MAX_IMAGE = 6       # 日志图片：每 IP 每分钟 6 张
LOG_EVENT_MAX_BYTES = 8192   # 单条事件 JSON 上限
LOG_IMAGE_MAX_BYTES = 12 * 1024 * 1024  # 12MB 单图上限（够 1024×3072 PNG）
PROXY_REQ_MAX_BYTES = 20 * 1024 * 1024  # 代理上行 20MB 上限（6 张参考图 + prompt）
JOB_START_MAX_BYTES = 20 * 1024 * 1024  # /api/jobs/start 上行同样上限

# —— 逆地理（Nominatim 代理）——
# 高德 Geocoder 只覆盖中国，海外坐标返回空；本路由用 OSM Nominatim 兜底。
# Nominatim 使用条款：必须自定义 UA + 缓存 + 不超过 1 QPS。
GEOCODE_TIMEOUT = 8
GEOCODE_RATE_WINDOW_SEC = 60
GEOCODE_RATE_MAX_PER_IP = 30           # 每 IP 每分钟 30 次（远低于点击节奏的上限）
GEOCODE_CACHE_TTL_SEC = 30 * 24 * 3600  # 30 天（地名半永久）
GEOCODE_CACHE_MAX_ENTRIES = 5000        # 进程内存上限

# —— 静态文件白名单 ——
# 不让客户端能拿到 .env / server.py / probe_*.py / e2e_*.bin / 任意 .log。
# 只暴露明确需要的资源。
STATIC_WHITELIST = {
    '/lilibear_logo.png',
}
STATIC_WHITELIST_PREFIXES = (
    '/refs_compressed/',
)
# 即便前缀匹配，也只放行这些后缀
STATIC_WHITELIST_EXTS = ('.jpg', '.jpeg', '.png', '.webp')

# —— 上游白名单 ——
# 仅允许图像生成相关路径，挡住其他 API 被滥用。
# 接受任意 namespace 前缀（dm-fox 的实际路径是 gptapi/v1/images/edits）
DMFOX_ALLOWED = re.compile(r'^([\w-]+/)*v1/images/(edits|generations)/?$')

PROXIES = {
    '/api/dmfox/': 'https://dm-fox.rjj.cc/',
}


# —— 加载 .env ——
def _load_env():
    env_path = os.path.join(DOC_ROOT, '.env')
    if not os.path.exists(env_path):
        return
    with open(env_path, 'r', encoding='utf-8') as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            k, v = line.split('=', 1)
            os.environ.setdefault(k.strip(), v.strip())


_load_env()
AMAP_KEY = os.environ.get('AMAP_KEY', '')
AMAP_SECURITY_CODE = os.environ.get('AMAP_SECURITY_CODE', '')
IMAGE_API_KEY = os.environ.get('IMAGE_API_KEY', '')

# —— 全球墙 / 共享 feed ——
# cookie 名固定，user_id 是 server 发的随机 token；昵称由用户自填
LW_COOKIE_NAME = 'lw_uid'
LW_COOKIE_MAX_AGE = 365 * 24 * 3600
NICKNAME_MAX_LEN = 16
# salt 用于 ip_hash；优先取环境变量，否则用 IMAGE_API_KEY 派生（缺也无所谓）
LW_IP_SALT = os.environ.get('LW_IP_SALT') or hashlib.sha256(
    ('ip-salt::' + (IMAGE_API_KEY or 'no-key')).encode('utf-8')
).hexdigest()[:32]
DB_PATH = os.path.join(DOC_ROOT, 'lilibear.db')
FEED_PAGE_DEFAULT = 20
FEED_PAGE_MAX = 60

TEMPLATE_SUBS = {
    b'__AMAP_KEY__': AMAP_KEY.encode('utf-8'),
    b'__AMAP_SECURITY_CODE__': AMAP_SECURITY_CODE.encode('utf-8'),
}

# —— 信任的反代来源 ——
# 当 self.client_address[0] 落在这里时，才信任 X-Real-IP / X-Forwarded-For。
# 生产场景：server.py 只监听 127.0.0.1，对外由本机 nginx 反代，所以信任本环回。
# 不要把任何外网地址加进来——否则客户端可以通过 X-F-F 伪造 IP 绕过限流。
TRUSTED_PROXY_IPS = {'127.0.0.1', '::1'}


# —— 请求/响应过滤 ——
# authorization 一定剥掉：上游 key 必须由 server 注入，绝不让客户端控制。
BLOCKED_REQ_HEADERS = {
    'host', 'origin', 'referer', 'connection', 'content-length',
    'sec-fetch-mode', 'sec-fetch-site', 'sec-fetch-dest',
    'authorization', 'x-forwarded-for', 'x-real-ip',
}
BLOCKED_RESP_HEADERS = {
    'connection', 'transfer-encoding',
    'access-control-allow-origin', 'access-control-allow-methods',
    'access-control-allow-headers', 'access-control-expose-headers',
    'vary',
}

# —— 限流状态 ——
_rate_lock = threading.Lock()
_rate_buckets = defaultdict(deque)
_log_event_buckets = defaultdict(deque)
_log_image_buckets = defaultdict(deque)
_geocode_buckets = defaultdict(deque)

# —— 逆地理缓存（进程内）——
_geocode_cache_lock = threading.Lock()
_geocode_cache = {}  # key="lat2,lng2" -> (saved_at, payload)

# —— 事件日志写入锁（多线程 server）——
_log_lock = threading.Lock()

# —— 任务表（进程内存）——
# job_id -> dict（完整 metadata）；图片 bytes 不放内存，只放磁盘路径
_jobs_lock = threading.Lock()
_jobs = {}
# 取消标志：set 包含 job_id 表示已请求取消；worker 在每个尝试边界检查
_jobs_cancel = set()

# —— SQLite ——
# WAL 模式 + check_same_thread=False，每个请求自建短连接（更简单可靠）
# 单 server 进程内并发由 SQLite 自己处理；写操作较少（每张图一次 INSERT），不会卡。
_db_init_lock = threading.Lock()
_db_inited = [False]


def _db_open():
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def _db_init():
    """启动时初始化：建表 + 启用 WAL。重复调用安全。"""
    with _db_init_lock:
        if _db_inited[0]:
            return
        conn = _db_open()
        try:
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA synchronous=NORMAL')  # WAL 下 NORMAL 已经足够安全
            conn.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id    TEXT PRIMARY KEY,
                    nickname   TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                )
            ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS images (
                    id            TEXT PRIMARY KEY,
                    job_id        TEXT,
                    trip_id       TEXT,
                    created_at    REAL NOT NULL,
                    lat           REAL,
                    lng           REAL,
                    place_short   TEXT,
                    place_full    TEXT,
                    size          TEXT,
                    duration_sec  REAL,
                    file_path     TEXT NOT NULL,
                    bytes         INTEGER,
                    user_id       TEXT NOT NULL,
                    visibility    TEXT NOT NULL DEFAULT 'public',
                    ip_hash       TEXT
                )
            ''')
            # 主索引：feed 按时间倒序拿公开图
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_images_feed
                ON images(created_at DESC)
            ''')
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_images_user
                ON images(user_id, created_at DESC)
            ''')
            conn.commit()
        finally:
            conn.close()
        _db_inited[0] = True


def _db_ensure_user(user_id):
    """user_id 不存在则插入空昵称记录。返回 (nickname, created_at)。"""
    conn = _db_open()
    try:
        cur = conn.execute('SELECT nickname, created_at FROM users WHERE user_id=?', (user_id,))
        row = cur.fetchone()
        if row:
            return row['nickname'], row['created_at']
        now = time.time()
        conn.execute(
            'INSERT INTO users(user_id, nickname, created_at, updated_at) VALUES(?, NULL, ?, ?)',
            (user_id, now, now),
        )
        conn.commit()
        return None, now
    finally:
        conn.close()


def _db_get_user(user_id):
    conn = _db_open()
    try:
        cur = conn.execute('SELECT user_id, nickname, created_at, updated_at FROM users WHERE user_id=?', (user_id,))
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def _db_set_nickname(user_id, nickname):
    conn = _db_open()
    try:
        now = time.time()
        conn.execute(
            'UPDATE users SET nickname=?, updated_at=? WHERE user_id=?',
            (nickname, now, user_id),
        )
        conn.commit()
    finally:
        conn.close()


def _db_insert_image(row):
    """row 是 dict，按表结构 INSERT。重复 id 直接报错（PRIMARY KEY 冲突）—— server 端 id 唯一所以不会撞。"""
    conn = _db_open()
    try:
        conn.execute('''
            INSERT INTO images
              (id, job_id, trip_id, created_at, lat, lng, place_short, place_full,
               size, duration_sec, file_path, bytes, user_id, visibility, ip_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            row['id'], row.get('job_id'), row.get('trip_id'), row['created_at'],
            row.get('lat'), row.get('lng'), row.get('place_short'), row.get('place_full'),
            row.get('size'), row.get('duration_sec'), row['file_path'], row.get('bytes'),
            row['user_id'], row.get('visibility') or 'public', row.get('ip_hash'),
        ))
        conn.commit()
    finally:
        conn.close()


def _db_get_image(image_id):
    conn = _db_open()
    try:
        cur = conn.execute('SELECT * FROM images WHERE id=?', (image_id,))
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def _db_set_image_visibility(image_id, user_id, visibility):
    """仅作者本人可改。返回更新后的行（或 None）。"""
    conn = _db_open()
    try:
        cur = conn.execute('SELECT user_id FROM images WHERE id=?', (image_id,))
        row = cur.fetchone()
        if not row:
            return None, 'not_found'
        if row['user_id'] != user_id:
            return None, 'forbidden'
        if visibility not in ('public', 'private'):
            return None, 'invalid'
        conn.execute('UPDATE images SET visibility=? WHERE id=?', (visibility, image_id))
        conn.commit()
        return _db_get_image(image_id), 'ok'
    finally:
        conn.close()


def _db_list_feed(viewer_user_id, cursor_ts, limit):
    """拉取 feed：公开图 + viewer 自己的私有图，按 created_at DESC。
    cursor_ts: float 或 None。limit: int。多取 1 条作"下一页 cursor"。"""
    conn = _db_open()
    try:
        params = []
        sql = '''
            SELECT i.*, u.nickname AS author_nickname
            FROM images i
            LEFT JOIN users u ON u.user_id = i.user_id
            WHERE (i.visibility='public' OR i.user_id=?)
        '''
        params.append(viewer_user_id)
        if cursor_ts is not None:
            sql += ' AND i.created_at < ?'
            params.append(cursor_ts)
        sql += ' ORDER BY i.created_at DESC LIMIT ?'
        params.append(limit + 1)  # 多取 1 条判断是否还有下一页
        cur = conn.execute(sql, tuple(params))
        rows = [dict(r) for r in cur.fetchall()]
        has_more = len(rows) > limit
        if has_more:
            rows = rows[:limit]
        next_cursor = rows[-1]['created_at'] if (rows and has_more) else None
        return rows, next_cursor
    finally:
        conn.close()


def _ip_hash(ip):
    return hashlib.sha256((LW_IP_SALT + '|' + (ip or '')).encode('utf-8')).hexdigest()[:24]


def _normalize_nickname(raw):
    """昵称清洗：去首尾空白、剥控制字符、限制长度。返回 (ok, cleaned 或 error_msg)。"""
    if not isinstance(raw, str):
        return False, '昵称必须是字符串'
    s = raw.strip()
    if not s:
        return False, '昵称不能为空'
    # 去控制字符（保留可见字符 + 空格 + 中日韩）
    s = ''.join(ch for ch in s if ch >= ' ' and ch != '\x7f')
    if len(s) > NICKNAME_MAX_LEN:
        return False, '昵称不能超过 %d 个字符' % NICKNAME_MAX_LEN
    # 防止 HTML / JSON 注入：禁止 < > " '
    if any(c in s for c in '<>"\''):
        return False, '昵称包含非法字符'
    return True, s


def _check_bucket(bucket_map, key, window_sec, limit):
    """通用滑动窗口。返回 (allowed, remaining)。"""
    now = time.time()
    with _rate_lock:
        bucket = bucket_map[key]
        while bucket and now - bucket[0] > window_sec:
            bucket.popleft()
        if len(bucket) >= limit:
            return False, 0
        bucket.append(now)
        return True, limit - len(bucket)


def _check_and_record_rate(ip):
    return _check_bucket(_rate_buckets, ip, RATE_WINDOW_SEC, RATE_MAX_PER_IP)


def _is_static_allowed(path):
    """白名单 + 拒绝目录穿越 + 限制后缀。"""
    # 拒绝任何包含 '..' 或 '~' 或 NULL 的路径
    if '..' in path or '\x00' in path or '~' in path:
        return False
    if path in STATIC_WHITELIST:
        return True
    for pre in STATIC_WHITELIST_PREFIXES:
        if path.startswith(pre):
            # 必须是有限后缀
            low = path.lower()
            return any(low.endswith(ext) for ext in STATIC_WHITELIST_EXTS)
    return False


def _safe_finite(v):
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)) and -1e9 < v < 1e9:
        return v
    return None


def _safe_str(v, max_len=200):
    if v is None:
        return ''
    s = str(v)
    # 去掉控制字符防止日志注入（保留换行交给 json 编码）
    s = ''.join(ch for ch in s if ch >= ' ' or ch in '\t')
    return s[:max_len]


def _ensure_log_dirs():
    os.makedirs(LOG_ROOT, exist_ok=True)
    os.makedirs(IMG_LOG_ROOT, exist_ok=True)
    os.makedirs(THUMB_ROOT, exist_ok=True)
    os.makedirs(JOB_LOG_ROOT, exist_ok=True)


def _thumb_path_for(image_id, day_str):
    """派生 thumb 文件路径。day_str 形如 '20260513'。caller 自己保证 day 一致性。"""
    d = os.path.join(THUMB_ROOT, day_str)
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, image_id + '.jpg')


def _generate_thumb(image_id, src_bytes, day_str):
    """同步生成缩略图并落盘。无 PIL 或失败时返回 None（caller 退化到原图）。
    KISS：不动 DB schema，路径由 (image_id, day) 派生 -> O(1) 命中。"""
    if not _HAS_PIL or not src_bytes:
        return None
    out_path = _thumb_path_for(image_id, day_str)
    try:
        with _PIL_Image.open(io.BytesIO(src_bytes)) as im:
            if im.mode in ('RGBA', 'LA', 'P'):
                im = im.convert('RGB')
            im.thumbnail((THUMB_MAX_SIDE, THUMB_MAX_SIDE), _PIL_Image.LANCZOS)
            im.save(out_path, 'JPEG', quality=THUMB_QUALITY, optimize=True)
        return out_path
    except Exception as e:
        print('[thumb] gen failed for %s: %s' % (image_id, e))
        # 半成品文件清理掉，避免 cache 命中坏图
        try:
            if os.path.exists(out_path):
                os.remove(out_path)
        except Exception:
            pass
        return None


def _write_event(record):
    _ensure_log_dirs()
    line = json.dumps(record, ensure_ascii=False)
    with _log_lock:
        with open(EVENT_LOG_PATH, 'a', encoding='utf-8') as f:
            f.write(line + '\n')


def _zh_first(s):
    """OSM 中文标签经常返回繁简并列（如 "美国;美國"、"东京都/東京都"、"韩国 / 南韓"），
    取分隔符前的第一段并去空白。"""
    if not s or not isinstance(s, str):
        return ''
    for sep in (';', '/', '／'):
        if sep in s:
            s = s.split(sep, 1)[0]
    return s.strip()


def _build_place_short(country, state, city, district):
    """根据国家/省/市/区数据生成短地名。
    国内：`市 · 区`（去掉市字），市级缺失时降级到省
    海外：`国 · 市`，市级缺失时退化到国家或省
    海洋：返回空字符串
    """
    country = country or ''
    state = state or ''
    city = city or ''
    district = district or ''
    if country == '中国':
        city_core = city.rstrip('市')
        if city_core and district:
            return city_core + ' · ' + district
        if city_core:
            return city_core
        if district:
            return district
        if state:
            return state.rstrip('省')
        return ''
    if country and city and city != country:
        return country + ' · ' + city
    if country and state and state != country:
        return country + ' · ' + state
    if country:
        return country
    if city:
        return city
    if state:
        return state
    return ''


# —— 任务持续化：辅助函数 ——
def _job_path(job_id):
    safe = re.sub(r'[^a-zA-Z0-9_-]', '', job_id)[:64]
    return os.path.join(JOB_LOG_ROOT, safe + '.json')


def _job_write_meta(job):
    """落盘 job metadata（不含图片）。原子写：先写 .tmp 再 rename。"""
    _ensure_log_dirs()
    p = _job_path(job['job_id'])
    tmp = p + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(job, f, ensure_ascii=False)
    os.replace(tmp, p)


def _job_load_meta(job_id):
    p = _job_path(job_id)
    if not os.path.exists(p):
        return None
    try:
        with open(p, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return None


def _job_get(job_id):
    """优先内存，回落到磁盘（server 重启后还能查老任务）。"""
    with _jobs_lock:
        j = _jobs.get(job_id)
        if j:
            return dict(j)
    return _job_load_meta(job_id)


def _job_update(job_id, **fields):
    with _jobs_lock:
        j = _jobs.get(job_id)
        if not j:
            return None
        j.update(fields)
        snapshot = dict(j)
    try:
        _job_write_meta(snapshot)
    except Exception as e:
        print('[jobs] meta write failed: %s' % e)
    return snapshot


def _job_set_status(job_id, status, **extra):
    """写状态时尊重已存在的终态：cancelled/done/failed 一旦写下就不再被覆盖。
    例外：cancelled 可以从 running/queued 进入；done/failed 不能覆盖 cancelled。"""
    fields = dict(extra)
    fields['status'] = status
    if status in ('done', 'failed', 'cancelled') and 'finished_at' not in fields:
        fields['finished_at'] = time.time()
    with _jobs_lock:
        j = _jobs.get(job_id)
        if not j:
            return None
        cur = j.get('status')
        if cur in ('done', 'failed', 'cancelled') and status != cur:
            # 已是终态，保留不动（防止 worker 跑完后覆盖用户取消）
            return dict(j)
        j.update(fields)
        snapshot = dict(j)
    try:
        _job_write_meta(snapshot)
    except Exception as e:
        print('[jobs] meta write failed: %s' % e)
    return snapshot


def _job_is_cancelled(job_id):
    with _jobs_lock:
        return job_id in _jobs_cancel


def _job_recover_on_startup():
    """启动时把磁盘上残留 status=running/queued 的 job 标记为 failed (interrupted)。
    server 死过一次，那条 fetch 一定已经断开，前端继续轮询应当看到失败。"""
    if not os.path.isdir(JOB_LOG_ROOT):
        return
    fixed = 0
    for fn in os.listdir(JOB_LOG_ROOT):
        if not fn.endswith('.json'):
            continue
        p = os.path.join(JOB_LOG_ROOT, fn)
        try:
            with open(p, 'r', encoding='utf-8') as f:
                j = json.load(f)
        except Exception:
            continue
        if j.get('status') in ('running', 'queued'):
            j['status'] = 'failed'
            j['error'] = 'server 重启，任务中断'
            j['finished_at'] = time.time()
            try:
                _job_write_meta(j)
                fixed += 1
            except Exception:
                pass
    if fixed:
        print('[jobs] recovered: %d interrupted job(s) marked failed' % fixed)


def _job_gc_loop():
    """周期清理：内存里超过 retention 的完成态任务移出；磁盘 metadata 仍保留供历史查询。"""
    while True:
        time.sleep(JOB_GC_INTERVAL_SEC)
        try:
            now = time.time()
            with _jobs_lock:
                drop = []
                for jid, j in _jobs.items():
                    fin = j.get('finished_at')
                    if fin and now - fin > JOB_RETENTION_SEC:
                        drop.append(jid)
                # 内存超出上限：按 finished_at 最老的丢
                if len(_jobs) > JOB_MAX_IN_MEMORY:
                    items = sorted(
                        ((jid, j) for jid, j in _jobs.items() if j.get('finished_at')),
                        key=lambda kv: kv[1].get('finished_at') or 0,
                    )
                    extra = len(_jobs) - JOB_MAX_IN_MEMORY
                    for jid, _j in items[:extra]:
                        drop.append(jid)
                for jid in drop:
                    _jobs.pop(jid, None)
                    _jobs_cancel.discard(jid)
        except Exception as e:
            print('[jobs] gc error: %s' % e)


def _job_run_upstream(job_id, ref_blobs, prompt, sizes, model):
    """后台 worker：依次尝试 sizes，调上游 dm-fox，落盘图片。
    任何 'size/dimension invalid' 类错误 → 切下一尺寸；其他错误（含限流/网络）→ 终止。"""
    if not IMAGE_API_KEY:
        _job_set_status(job_id, 'failed', error='IMAGE_API_KEY 未配置')
        return

    upstream_url = 'https://dm-fox.rjj.cc/gptapi/v1/images/edits'
    last_err = None
    ctx = ssl.create_default_context()

    for size in sizes:
        if _job_is_cancelled(job_id):
            _job_set_status(job_id, 'cancelled', error='用户取消')
            return

        # 构造 multipart body（自己拼，避免引第三方依赖）
        boundary = '----lilibear-' + uuid.uuid4().hex
        body = _build_multipart(boundary, [
            ('model', None, model.encode('utf-8')),
            ('prompt', None, prompt.encode('utf-8')),
            ('size', None, size.encode('utf-8')),
            ('n', None, b'1'),
        ] + [('image', 'ref%d.jpg' % i, b) for i, b in enumerate(ref_blobs)])

        headers = {
            'Authorization': 'Bearer ' + IMAGE_API_KEY,
            'Content-Type': 'multipart/form-data; boundary=' + boundary,
            'Content-Length': str(len(body)),
        }

        attempts = RETRY_TIMES + 1
        for attempt in range(attempts):
            if _job_is_cancelled(job_id):
                _job_set_status(job_id, 'cancelled', error='用户取消')
                return
            try:
                req = urllib.request.Request(upstream_url, data=body, headers=headers, method='POST')
                with urllib.request.urlopen(req, context=ctx, timeout=UPSTREAM_TIMEOUT) as resp:
                    raw = resp.read()
                try:
                    j = json.loads(raw.decode('utf-8'))
                except Exception:
                    j = None
                if not j or 'data' not in j or not j['data']:
                    last_err = '返回无图片数据'
                    break
                item = j['data'][0]
                b64 = item.get('b64_json')
                url = item.get('url')
                if b64:
                    img_bytes = base64.b64decode(b64)
                elif url:
                    with urllib.request.urlopen(url, context=ctx, timeout=UPSTREAM_TIMEOUT) as ir:
                        img_bytes = ir.read()
                else:
                    last_err = '未识别的返回格式'
                    break
                if _job_is_cancelled(job_id):
                    # 已取消：丢弃结果，不落盘
                    _job_set_status(job_id, 'cancelled', error='用户取消')
                    return
                # 落盘到 logs/images/YYYYMMDD/
                meta = _job_get(job_id) or {}
                lat = meta.get('lat')
                lng = meta.get('lng')
                trip_id = meta.get('trip_id') or ''
                _ensure_log_dirs()
                day = datetime.datetime.now().strftime('%Y%m%d')
                day_dir = os.path.join(IMG_LOG_ROOT, day)
                os.makedirs(day_dir, exist_ok=True)
                # 用 png 还是 jpg：看签名
                ext = '.png' if img_bytes[:8] == b'\x89PNG\r\n\x1a\n' else '.jpg'
                safe_id = re.sub(r'[^a-zA-Z0-9_-]', '', trip_id)[:24] or job_id[:12]
                lat_s = ('%+.4f' % lat) if isinstance(lat, (int, float)) else 'NA'
                lng_s = ('%+.4f' % lng) if isinstance(lng, (int, float)) else 'NA'
                fname = '%s_%s_%s_%s%s' % (
                    datetime.datetime.now().strftime('%H%M%S'), lat_s, lng_s, safe_id, ext)
                fpath = os.path.join(day_dir, fname)
                with open(fpath, 'wb') as f:
                    f.write(img_bytes)
                rel = os.path.relpath(fpath, DOC_ROOT).replace(os.sep, '/')
                started = meta.get('started_at') or time.time()
                duration = round(time.time() - started, 2)
                # 给这张图分配一个全局 image_id，写入 images 表（feed 数据源）
                image_id = uuid.uuid4().hex[:24]
                _job_set_status(
                    job_id, 'done',
                    image_id=image_id,
                    image_path=rel, image_bytes=len(img_bytes),
                    size=size, duration_sec=duration,
                )
                try:
                    _db_insert_image({
                        'id': image_id,
                        'job_id': job_id,
                        'trip_id': trip_id,
                        'created_at': time.time(),
                        'lat': lat, 'lng': lng,
                        'place_short': meta.get('place_short') or '',
                        'place_full':  meta.get('place_full')  or '',
                        'size': size,
                        'duration_sec': duration,
                        'file_path': rel,
                        'bytes': len(img_bytes),
                        'user_id': meta.get('user_id') or '',
                        'visibility': meta.get('visibility') or 'public',
                        'ip_hash': meta.get('ip_hash'),
                    })
                except Exception as e:
                    # DB 写失败不该阻断流程 —— 任务已 done，前端能继续；只是 feed 漏一条
                    print('[jobs] insert image failed: %s' % e)
                # 同步生成缩略图（feed/markers 用）；失败不阻塞，/api/feed/thumb 会 lazy 兜底
                try:
                    _generate_thumb(image_id, img_bytes, day)
                except Exception as e:
                    print('[thumb] sync gen on job done failed: %s' % e)
                _write_event({
                    'ts': time.time(), 'type': 'job_done',
                    'job_id': job_id, 'trip_id': trip_id, 'image_id': image_id,
                    'lat': lat, 'lng': lng, 'size': size,
                    'duration_sec': duration, 'path': rel, 'bytes': len(img_bytes),
                })
                return
            except urllib.error.HTTPError as e:
                err_text = ''
                try: err_text = e.read().decode('utf-8', errors='replace')[:500]
                except Exception: pass
                err_label = 'HTTP %d %s' % (e.code, err_text)
                last_err = err_label
                if e.code == 429:
                    # 限流 → 不再重试此尺寸，也不切下个尺寸；直接失败
                    _job_set_status(job_id, 'failed', error='限流命中：' + err_label)
                    return
                # size/dimension invalid → 切下一尺寸
                if re.search(r'size|dimension|invalid|aspect', err_text, re.I):
                    break
                if e.code in (502, 503, 504, 408) and attempt < attempts - 1:
                    time.sleep(RETRY_BACKOFF_BASE * (2 ** attempt))
                    continue
                break
            except Exception as e:
                err_label = type(e).__name__ + ': ' + str(e)
                last_err = err_label
                if attempt < attempts - 1:
                    time.sleep(RETRY_BACKOFF_BASE * (2 ** attempt))
                    continue
                break

    _job_set_status(job_id, 'failed', error=last_err or '生成失败')
    _write_event({
        'ts': time.time(), 'type': 'job_failed',
        'job_id': job_id, 'msg': (last_err or '')[:500],
    })


def _detect_image_mime(data):
    """按字节签名判断真实图像 MIME。
    必须给出真实类型：dm-fox 会把我们 multipart 里的 Content-Type 透传给下游模型，
    下游的 vision API（Claude / GPT）拒绝 application/octet-stream。"""
    if not data or len(data) < 12:
        return 'application/octet-stream'
    if data[:8] == b'\x89PNG\r\n\x1a\n':
        return 'image/png'
    if data[:2] == b'\xff\xd8':
        return 'image/jpeg'
    if data[:6] in (b'GIF87a', b'GIF89a'):
        return 'image/gif'
    if data[:4] == b'RIFF' and data[8:12] == b'WEBP':
        return 'image/webp'
    return 'application/octet-stream'


def _build_multipart(boundary, fields):
    """fields: list[(name, filename_or_None, bytes)].
    文件部分 Content-Type 按字节签名识别（必须给真实 image/*；上游会透传给 vision API）。"""
    out = []
    bnd = ('--' + boundary).encode('latin-1')
    for name, fn, data in fields:
        out.append(bnd + b'\r\n')
        if fn:
            ctype = _detect_image_mime(data) if isinstance(data, (bytes, bytearray)) else 'application/octet-stream'
            disp = 'form-data; name="%s"; filename="%s"' % (name, fn)
            out.append(('Content-Disposition: ' + disp + '\r\n').encode('latin-1'))
            out.append(('Content-Type: ' + ctype + '\r\n\r\n').encode('latin-1'))
        else:
            out.append(('Content-Disposition: form-data; name="%s"\r\n\r\n' % name).encode('latin-1'))
        out.append(data)
        out.append(b'\r\n')
    out.append(('--' + boundary + '--\r\n').encode('latin-1'))
    return b''.join(out)


def _parse_multipart(headers, body):
    """极简 multipart/form-data 解析。返回 dict[name] -> (filename or None, bytes)。"""
    ctype = headers.get('Content-Type', '')
    m = re.search(r'boundary=([^;]+)', ctype)
    if not m:
        return None
    boundary = ('--' + m.group(1).strip().strip('"')).encode('latin-1')
    parts = body.split(boundary)
    out = {}
    for p in parts:
        if not p or p in (b'--\r\n', b'--'):
            continue
        p = p.lstrip(b'\r\n')
        if p.endswith(b'\r\n'):
            p = p[:-2]
        if p.endswith(b'--'):
            p = p[:-2]
        sep = p.find(b'\r\n\r\n')
        if sep == -1:
            continue
        head_blob = p[:sep].decode('latin-1', errors='replace')
        data = p[sep + 4:]
        if data.endswith(b'\r\n'):
            data = data[:-2]
        dispo = ''
        for line in head_blob.split('\r\n'):
            if line.lower().startswith('content-disposition'):
                dispo = line
                break
        nm = re.search(r'name="([^"]*)"', dispo)
        fn = re.search(r'filename="([^"]*)"', dispo)
        if not nm:
            continue
        out[nm.group(1)] = (fn.group(1) if fn else None, data)
    return out


class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        self._user_id = None  # 由 _ensure_user_cookie() 在每个请求开始时填充
        self._set_cookie_user_id = None  # 需要在响应里发 Set-Cookie 的 user_id（新分配时）
        super().__init__(*args, directory=DOC_ROOT, **kwargs)

    def log_message(self, fmt, *args):
        sys.stdout.write("[%s] %s\n" % (self.log_date_time_string(), fmt % args))
        sys.stdout.flush()

    # ---------- 工具 ----------
    def _client_ip(self):
        """真实客户端 IP。
        生产走 nginx 反代时 client_address 永远是 127.0.0.1，导致所有用户被并到同一个
        限流桶（之前的 bug）；这里在来源是本机时改读 X-Real-IP（nginx 注入 $remote_addr，
        不接收客户端 header），退化时 X-Forwarded-For 取最右一个值（最右才是上游 nginx
        附加的真实 IP，最左可能是攻击者伪造的）。
        """
        direct = self.client_address[0] if self.client_address else 'unknown'
        if direct not in TRUSTED_PROXY_IPS:
            return direct
        xri = (self.headers.get('X-Real-IP') or '').strip()
        if xri:
            return xri
        xff = self.headers.get('X-Forwarded-For') or ''
        parts = [p.strip() for p in xff.split(',') if p.strip()]
        if parts:
            return parts[-1]   # 最右 = nginx 附加的 $remote_addr，可信；最左可被客户端伪造
        return direct

    def _parse_cookies(self):
        raw = self.headers.get('Cookie', '')
        if not raw:
            return {}
        try:
            c = SimpleCookie()
            c.load(raw)
            return {k: v.value for k, v in c.items()}
        except Exception:
            return {}

    def _ensure_user_cookie(self):
        """读 cookie 拿 user_id；没有就发新的。把结果存到 self._user_id。
        如果是新分配，self._set_cookie_user_id 也会被设置，让响应去发 Set-Cookie。
        每个 do_X 入口处调用。"""
        cookies = self._parse_cookies()
        uid = cookies.get(LW_COOKIE_NAME, '')
        # 校验格式：32 hex chars
        if not re.match(r'^[a-f0-9]{32}$', uid or ''):
            uid = uuid.uuid4().hex  # 32 chars
            self._set_cookie_user_id = uid
        self._user_id = uid
        try:
            _db_ensure_user(uid)
        except Exception as e:
            print('[user] ensure failed: %s' % e)

    def _cookie_header_value(self):
        if not self._set_cookie_user_id:
            return None
        return '%s=%s; Max-Age=%d; Path=/; HttpOnly; SameSite=Lax' % (
            LW_COOKIE_NAME, self._set_cookie_user_id, LW_COOKIE_MAX_AGE,
        )

    def end_headers(self):
        # 自动给所有响应注入 Set-Cookie（仅在本次请求是新分配 user_id 时）
        ck = self._cookie_header_value()
        if ck:
            self.send_header('Set-Cookie', ck)
            self._set_cookie_user_id = None  # 一次性
        super().end_headers()

    def _cors_204(self):
        self.send_response(204)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET,POST,OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.send_header('Access-Control-Max-Age', '86400')
        self.end_headers()

    def _json_response(self, status, payload):
        body = json.dumps(payload, ensure_ascii=False).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except Exception:
            pass

    def _read_body(self, max_bytes):
        content_length = int(self.headers.get('Content-Length', '0') or '0')
        if content_length <= 0:
            return b''
        if content_length > max_bytes:
            return None
        return self.rfile.read(content_length)

    # ---------- 代理 ----------
    def _matched_prefix(self):
        for prefix in PROXIES:
            if self.path.startswith(prefix):
                return prefix
        return None

    def _proxy(self, prefix):
        upstream_base = PROXIES[prefix]
        upstream_path = self.path[len(prefix):].split('?', 1)[0]
        # 路径白名单：仅 /v1/images/edits|generations
        if prefix == '/api/dmfox/' and not DMFOX_ALLOWED.match(upstream_path):
            self._json_response(403, {'error': {'message': 'forbidden upstream path'}})
            return
        if prefix == '/api/dmfox/' and not IMAGE_API_KEY:
            self._json_response(503, {'error': {'message': 'IMAGE_API_KEY 未配置在 .env 中'}})
            return
        # 上行大小校验：防止有人塞 1GB 把 server 进程撑爆
        content_length = int(self.headers.get('Content-Length', '0') or '0')
        if content_length < 0 or content_length > PROXY_REQ_MAX_BYTES:
            self._json_response(413, {'error': {'message': '上行请求过大'}})
            return
        # 限流
        ip = self._client_ip()
        allowed, remaining = _check_and_record_rate(ip)
        if not allowed:
            _write_event({
                'ts': time.time(), 'type': 'rate_limit', 'ip': ip,
                'path': self.path[:200],
            })
            self._json_response(429, {'error': {
                'message': '限流：每 IP %d 次/%ds' % (RATE_MAX_PER_IP, RATE_WINDOW_SEC),
            }})
            return

        upstream_url = upstream_base + self.path[len(prefix):]

        body = self.rfile.read(content_length) if content_length > 0 else None

        # 透传请求头（剥掉敏感头），然后注入 Authorization
        headers = {}
        for k, v in self.headers.items():
            if k.lower() in BLOCKED_REQ_HEADERS:
                continue
            headers[k] = v
        if prefix == '/api/dmfox/':
            headers['Authorization'] = 'Bearer ' + IMAGE_API_KEY

        ctx = ssl.create_default_context()
        attempts = RETRY_TIMES + 1
        attempt_log = []

        for attempt in range(attempts):
            try:
                req = urllib.request.Request(upstream_url, data=body, headers=headers, method=self.command)
                with urllib.request.urlopen(req, context=ctx, timeout=UPSTREAM_TIMEOUT) as resp:
                    self.send_response(resp.status)
                    for k, v in resp.headers.items():
                        if k.lower() in BLOCKED_RESP_HEADERS:
                            continue
                        self.send_header(k, v)
                    self.send_header('Access-Control-Allow-Origin', '*')
                    self.send_header('Access-Control-Expose-Headers', '*')
                    self.send_header('X-RateLimit-Remaining', str(remaining))
                    if attempt > 0:
                        self.send_header('X-Proxy-Attempts', str(attempt + 1))
                    self.end_headers()
                    while True:
                        chunk = resp.read(8192)
                        if not chunk:
                            break
                        try:
                            self.wfile.write(chunk)
                        except (BrokenPipeError, ConnectionResetError):
                            return
                    if attempt > 0:
                        print('[proxy] succeeded on attempt %d/%d for %s' % (attempt + 1, attempts, upstream_url))
                    return
            except urllib.error.HTTPError as e:
                err_label = 'HTTP %d' % e.code
                attempt_log.append(err_label)
                if e.code in (502, 503, 504, 408, 429) and attempt < attempts - 1:
                    backoff = RETRY_BACKOFF_BASE * (2 ** attempt)
                    print('[proxy] %s on %s, retry %d/%d in %.1fs' % (
                        err_label, upstream_url, attempt + 1, attempts - 1, backoff))
                    time.sleep(backoff)
                    continue
                self.send_response(e.code)
                for k, v in e.headers.items():
                    if k.lower() in BLOCKED_RESP_HEADERS:
                        continue
                    self.send_header(k, v)
                self.send_header('Access-Control-Allow-Origin', '*')
                self.send_header('X-Proxy-Attempts', str(attempt + 1))
                self.end_headers()
                try:
                    self.wfile.write(e.read())
                except Exception:
                    pass
                return
            except Exception as e:
                err_label = type(e).__name__ + ': ' + str(e)
                attempt_log.append(err_label)
                if attempt < attempts - 1:
                    backoff = RETRY_BACKOFF_BASE * (2 ** attempt)
                    print('[proxy] %s on %s, retry %d/%d in %.1fs' % (
                        err_label, upstream_url, attempt + 1, attempts - 1, backoff))
                    time.sleep(backoff)
                    continue
                self.send_response(502)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.send_header('X-Proxy-Attempts', str(attempt + 1))
                self.end_headers()
                err_summary = '; '.join(attempt_log[-3:])
                hint = ''
                low = err_summary.lower()
                if 'unexpected_eof_while_reading' in low or 'sslerror' in low:
                    hint = ' （TLS 连接被中间设备 reset，本地网络到上游链路不稳）'
                elif 'timeout' in low:
                    hint = ' （上游响应超时）'
                msg = {
                    'error': {
                        'message': '代理 %d 次重试均失败: %s%s' % (attempts, err_summary, hint),
                        'attempts': attempts,
                    }
                }
                try:
                    self.wfile.write(json.dumps(msg, ensure_ascii=False).encode('utf-8'))
                except Exception:
                    pass
                return

    # ---------- 日志端点 ----------
    def _handle_log_event(self):
        ip = self._client_ip()
        ok, _ = _check_bucket(_log_event_buckets, ip, LOG_RATE_WINDOW_SEC, LOG_RATE_MAX_EVENT)
        if not ok:
            self._json_response(429, {'error': {'message': 'log event 限流'}})
            return
        body = self._read_body(LOG_EVENT_MAX_BYTES)
        if body is None:
            self._json_response(413, {'error': {'message': 'event payload too large'}})
            return
        try:
            data = json.loads(body.decode('utf-8'))
        except Exception:
            self._json_response(400, {'error': {'message': 'invalid json'}})
            return
        if not isinstance(data, dict):
            self._json_response(400, {'error': {'message': 'expected json object'}})
            return
        record = {
            'ts': time.time(),
            'ip': self._client_ip(),
            'ua': _safe_str(self.headers.get('User-Agent', ''), 200),
            'type': _safe_str(data.get('type'), 32),
            'tab_id': _safe_str(data.get('tab_id'), 64),
            'trip_id': _safe_str(data.get('trip_id'), 64),
            'lat': _safe_finite(data.get('lat')),
            'lng': _safe_finite(data.get('lng')),
            'size': _safe_str(data.get('size'), 32),
            'duration_sec': _safe_finite(data.get('duration_sec')),
            'view': _safe_str(data.get('view'), 16),
            'msg': _safe_str(data.get('msg'), 500),
        }
        try:
            _write_event(record)
        except Exception as e:
            self._json_response(500, {'error': {'message': 'log write failed: ' + str(e)}})
            return
        self._json_response(204, {})

    def _handle_log_image(self):
        ip = self._client_ip()
        ok, _ = _check_bucket(_log_image_buckets, ip, LOG_RATE_WINDOW_SEC, LOG_RATE_MAX_IMAGE)
        if not ok:
            self._json_response(429, {'error': {'message': 'log image 限流'}})
            return
        # multipart 字段: image (file), trip_id, lat, lng
        content_length = int(self.headers.get('Content-Length', '0') or '0')
        if content_length <= 0 or content_length > LOG_IMAGE_MAX_BYTES:
            self._json_response(413, {'error': {'message': 'image payload too large or empty'}})
            return
        body = self.rfile.read(content_length)
        parts = _parse_multipart(self.headers, body)
        if not parts or 'image' not in parts:
            self._json_response(400, {'error': {'message': 'missing image part'}})
            return
        _, img_bytes = parts['image']
        if not img_bytes:
            self._json_response(400, {'error': {'message': 'empty image'}})
            return
        # 简单签名校验：PNG / JPEG 头
        if not (img_bytes[:8] == b'\x89PNG\r\n\x1a\n' or img_bytes[:3] == b'\xff\xd8\xff'):
            self._json_response(400, {'error': {'message': 'unsupported image format'}})
            return
        ext = '.png' if img_bytes[:8] == b'\x89PNG\r\n\x1a\n' else '.jpg'

        def _decode_field(name):
            t = parts.get(name)
            if not t:
                return ''
            return t[1].decode('utf-8', errors='replace')

        lat = _safe_finite(_to_float(_decode_field('lat')))
        lng = _safe_finite(_to_float(_decode_field('lng')))
        trip_id = _safe_str(_decode_field('trip_id'), 64)
        size = _safe_str(_decode_field('size'), 32)
        duration = _safe_finite(_to_float(_decode_field('duration_sec')))

        _ensure_log_dirs()
        day = datetime.datetime.now().strftime('%Y%m%d')
        day_dir = os.path.join(IMG_LOG_ROOT, day)
        os.makedirs(day_dir, exist_ok=True)
        # 文件名稳定可追溯：时间戳_lat_lng_id.ext
        safe_id = re.sub(r'[^a-zA-Z0-9_-]', '', trip_id)[:24] or uuid.uuid4().hex[:12]
        lat_s = ('%+.4f' % lat) if lat is not None else 'NA'
        lng_s = ('%+.4f' % lng) if lng is not None else 'NA'
        fname = '%s_%s_%s_%s%s' % (
            datetime.datetime.now().strftime('%H%M%S'), lat_s, lng_s, safe_id, ext)
        fpath = os.path.join(day_dir, fname)
        with open(fpath, 'wb') as f:
            f.write(img_bytes)
        rel = os.path.relpath(fpath, DOC_ROOT).replace(os.sep, '/')
        _write_event({
            'ts': time.time(), 'type': 'image_saved', 'ip': self._client_ip(),
            'trip_id': trip_id, 'lat': lat, 'lng': lng, 'size': size,
            'duration_sec': duration, 'path': rel,
            'bytes': len(img_bytes),
        })
        self._json_response(200, {'ok': True, 'path': rel, 'bytes': len(img_bytes)})

    # ---------- 任务系统（持续化生成）----------
    def _handle_job_start(self):
        """POST /api/jobs/start
        multipart 字段：model, prompt, size 或 sizes(JSON 数组), n, tab_id, trip_id,
                        lat, lng, place_short, place_full + 多个 image 文件
        立即返回 {job_id, status:'running'}，后台线程跑上游。"""
        ip = self._client_ip()
        # 共用代理的限流（同一上游 quota）
        allowed, _ = _check_and_record_rate(ip)
        if not allowed:
            self._json_response(429, {'error': {
                'message': '限流：每 IP %d 次/%ds' % (RATE_MAX_PER_IP, RATE_WINDOW_SEC),
            }})
            return
        content_length = int(self.headers.get('Content-Length', '0') or '0')
        if content_length <= 0 or content_length > JOB_START_MAX_BYTES:
            self._json_response(413, {'error': {'message': '上行请求过大或为空'}})
            return
        if not IMAGE_API_KEY:
            self._json_response(503, {'error': {'message': 'IMAGE_API_KEY 未配置在 .env 中'}})
            return
        body = self.rfile.read(content_length)
        parts = _parse_multipart(self.headers, body)
        if not parts:
            self._json_response(400, {'error': {'message': 'invalid multipart'}})
            return

        def field(name):
            t = parts.get(name)
            return t[1].decode('utf-8', errors='replace') if t else ''

        prompt = field('prompt')
        if not prompt:
            self._json_response(400, {'error': {'message': 'missing prompt'}})
            return
        model = field('model') or 'gpt-image-2'
        size_one = field('size')
        sizes_raw = field('sizes')
        sizes = None
        if sizes_raw:
            try:
                arr = json.loads(sizes_raw)
                if isinstance(arr, list):
                    sizes = [str(s) for s in arr if isinstance(s, str) and re.match(r'^\d{3,5}x\d{3,5}$', s)]
            except Exception:
                sizes = None
        if not sizes:
            sizes = [size_one] if re.match(r'^\d{3,5}x\d{3,5}$', size_one or '') else ['1024x3072']

        tab_id = _safe_str(field('tab_id'), 64)
        trip_id = _safe_str(field('trip_id'), 64)
        lat = _safe_finite(_to_float(field('lat')))
        lng = _safe_finite(_to_float(field('lng')))
        place_short = _safe_str(field('place_short'), 200)
        place_full  = _safe_str(field('place_full'), 500)
        visibility = field('visibility') or 'public'
        if visibility not in ('public', 'private'):
            visibility = 'public'

        # 收集 image 部件（_parse_multipart 用 name 作 key 会让多个 image 互相覆盖；
        # 这里换成扫一遍 parts 字典——但当前 _parse_multipart 同名会覆盖）
        # 既然现实里前端会重命名为 image / 多次出现的 same name，需要修一下
        # 简化：让前端用 image0, image1, ... 命名
        ref_blobs = []
        for i in range(20):
            k = 'image%d' % i
            t = parts.get(k)
            if not t:
                break
            ref_blobs.append(t[1])
        if not ref_blobs:
            self._json_response(400, {'error': {'message': '至少需要一张参考图 image0'}})
            return

        job_id = uuid.uuid4().hex[:16]
        now = time.time()
        job = {
            'job_id': job_id,
            'tab_id': tab_id,
            'trip_id': trip_id,
            'lat': lat, 'lng': lng,
            'place_short': place_short,
            'place_full': place_full,
            'visibility': visibility,
            'user_id': self._user_id,
            'ip_hash': _ip_hash(ip),
            'model': model,
            'sizes': sizes,
            'created_at': now,
            'started_at': now,
            'finished_at': None,
            'status': 'running',
            'error': None,
            'image_id': None,
            'image_path': None,
            'image_bytes': None,
            'duration_sec': None,
            'size': None,
            'ip': ip,
        }
        with _jobs_lock:
            _jobs[job_id] = job
        try:
            _job_write_meta(job)
        except Exception:
            pass

        _write_event({
            'ts': now, 'type': 'job_start',
            'job_id': job_id, 'trip_id': trip_id, 'tab_id': tab_id,
            'lat': lat, 'lng': lng,
        })

        t = threading.Thread(
            target=_job_run_upstream,
            args=(job_id, ref_blobs, prompt, sizes, model),
            daemon=True,
        )
        t.start()
        self._json_response(200, {'job_id': job_id, 'status': 'running'})

    def _job_id_from_path(self, prefix):
        """从路径里抽 job_id。prefix 形如 '/api/jobs/'，path 形如 '/api/jobs/{id}' 或 '/api/jobs/{id}/image'。"""
        p = self.path.split('?', 1)[0]
        if not p.startswith(prefix):
            return None
        tail = p[len(prefix):]
        seg = tail.split('/', 1)[0]
        if not re.match(r'^[a-f0-9]{8,32}$', seg):
            return None
        return seg

    def _handle_job_get(self, job_id):
        j = _job_get(job_id)
        if not j:
            self._json_response(404, {'error': {'message': 'job not found'}})
            return
        # 不返回 ip 字段（无意义且属于元信息）
        out = {k: v for k, v in j.items() if k != 'ip'}
        self._json_response(200, out)

    def _handle_job_image(self, job_id):
        j = _job_get(job_id)
        if not j:
            self._json_response(404, {'error': {'message': 'job not found'}})
            return
        if j.get('status') != 'done' or not j.get('image_path'):
            self._json_response(409, {'error': {'message': 'image not ready', 'status': j.get('status')}})
            return
        rel = j['image_path']
        # 安全：image_path 必须在 IMG_LOG_ROOT 下
        abs_path = os.path.normpath(os.path.join(DOC_ROOT, rel))
        if not abs_path.startswith(os.path.normpath(IMG_LOG_ROOT) + os.sep):
            self._json_response(403, {'error': {'message': 'forbidden path'}})
            return
        if not os.path.exists(abs_path):
            self._json_response(404, {'error': {'message': 'image file missing'}})
            return
        try:
            with open(abs_path, 'rb') as f:
                data = f.read()
        except Exception as e:
            self._json_response(500, {'error': {'message': 'read failed: ' + str(e)}})
            return
        ctype = 'image/png' if data[:8] == b'\x89PNG\r\n\x1a\n' else 'image/jpeg'
        self.send_response(200)
        self.send_header('Content-Type', ctype)
        self.send_header('Content-Length', str(len(data)))
        self.send_header('Cache-Control', 'private, max-age=3600')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        try:
            self.wfile.write(data)
        except Exception:
            pass

    def _handle_job_cancel(self, job_id):
        j = _job_get(job_id)
        if not j:
            self._json_response(404, {'error': {'message': 'job not found'}})
            return
        if j.get('status') in ('done', 'failed', 'cancelled'):
            self._json_response(200, {'ok': True, 'status': j['status'], 'noop': True})
            return
        with _jobs_lock:
            _jobs_cancel.add(job_id)
        # worker 会在下一个尝试边界检测并落 status=cancelled
        # 但若 worker 当前阻塞在 urlopen 里，则要等上游返回才会真正停。
        # 立即把 meta 标 cancelled 提示前端（worker 拿到后会跳过结果落盘）
        _job_set_status(job_id, 'cancelled', error='用户取消')
        _write_event({
            'ts': time.time(), 'type': 'job_cancel', 'job_id': job_id,
        })
        self._json_response(200, {'ok': True, 'status': 'cancelled'})

    # ---------- 全球墙 / 共享 feed ----------
    def _handle_me_get(self):
        u = _db_get_user(self._user_id) or {}
        self._json_response(200, {
            'user_id': self._user_id,
            'nickname': u.get('nickname'),
        })

    def _handle_me_post(self):
        body = self._read_body(2048)
        if body is None:
            self._json_response(413, {'error': {'message': 'body too large'}})
            return
        try:
            data = json.loads(body.decode('utf-8'))
        except Exception:
            self._json_response(400, {'error': {'message': 'invalid json'}})
            return
        ok, cleaned_or_msg = _normalize_nickname(data.get('nickname'))
        if not ok:
            self._json_response(400, {'error': {'message': cleaned_or_msg}})
            return
        _db_set_nickname(self._user_id, cleaned_or_msg)
        self._json_response(200, {'user_id': self._user_id, 'nickname': cleaned_or_msg})

    def _handle_feed_list(self):
        qs = self.path.split('?', 1)[1] if '?' in self.path else ''
        params = urllib.parse.parse_qs(qs)
        cursor = _to_float((params.get('cursor') or [''])[0])
        try:
            limit = int((params.get('limit') or [str(FEED_PAGE_DEFAULT)])[0])
        except Exception:
            limit = FEED_PAGE_DEFAULT
        limit = max(1, min(limit, FEED_PAGE_MAX))
        rows, next_cursor = _db_list_feed(self._user_id, cursor, limit)
        out = []
        for r in rows:
            out.append({
                'id': r['id'],
                'created_at': r['created_at'],
                'lat': r['lat'], 'lng': r['lng'],
                'place_short': r['place_short'] or '',
                'place_full':  r['place_full']  or '',
                'size': r['size'] or '',
                'duration_sec': r['duration_sec'],
                'user_id': r['user_id'],
                'is_mine': r['user_id'] == self._user_id,
                'author_nickname': r.get('author_nickname') or '',
                'visibility': r['visibility'],
            })
        self._json_response(200, {
            'items': out,
            'next_cursor': next_cursor,
        })

    def _handle_feed_image(self, image_id):
        """读 images 表拿 file_path，鉴权后回 binary。公开图任意可见；私有图仅作者本人可看。"""
        row = _db_get_image(image_id)
        if not row:
            self._json_response(404, {'error': {'message': 'image not found'}})
            return
        if row['visibility'] != 'public' and row['user_id'] != self._user_id:
            self._json_response(403, {'error': {'message': 'forbidden'}})
            return
        rel = row['file_path']
        abs_path = os.path.normpath(os.path.join(DOC_ROOT, rel))
        if not abs_path.startswith(os.path.normpath(IMG_LOG_ROOT) + os.sep):
            self._json_response(403, {'error': {'message': 'forbidden path'}})
            return
        if not os.path.exists(abs_path):
            self._json_response(404, {'error': {'message': 'image file missing'}})
            return
        try:
            with open(abs_path, 'rb') as f:
                data = f.read()
        except Exception as e:
            self._json_response(500, {'error': {'message': 'read failed: ' + str(e)}})
            return
        ctype = 'image/png' if data[:8] == b'\x89PNG\r\n\x1a\n' else 'image/jpeg'
        self.send_response(200)
        self.send_header('Content-Type', ctype)
        self.send_header('Content-Length', str(len(data)))
        self.send_header('Cache-Control', 'public, max-age=86400')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        try:
            self.wfile.write(data)
        except Exception:
            pass

    def _handle_feed_thumb(self, image_id):
        """缩略图：lazy 生成 + 落盘 cache。无 PIL 时退化为原图（不会 break）。"""
        row = _db_get_image(image_id)
        if not row:
            self._json_response(404, {'error': {'message': 'image not found'}})
            return
        if row['visibility'] != 'public' and row['user_id'] != self._user_id:
            self._json_response(403, {'error': {'message': 'forbidden'}})
            return
        # 用 created_at 推 day_str（thumb 路径与原图同 day 目录），保证幂等
        try:
            day_str = datetime.datetime.fromtimestamp(row['created_at']).strftime('%Y%m%d')
        except Exception:
            day_str = datetime.datetime.now().strftime('%Y%m%d')
        tp = _thumb_path_for(image_id, day_str)
        if not os.path.exists(tp):
            # 第一次访问 → 现做。读源图前先做路径白名单检查。
            rel = row['file_path']
            abs_path = os.path.normpath(os.path.join(DOC_ROOT, rel))
            if not abs_path.startswith(os.path.normpath(IMG_LOG_ROOT) + os.sep):
                self._json_response(403, {'error': {'message': 'forbidden path'}})
                return
            if not os.path.exists(abs_path):
                self._json_response(404, {'error': {'message': 'image file missing'}})
                return
            try:
                with open(abs_path, 'rb') as f:
                    src = f.read()
            except Exception as e:
                self._json_response(500, {'error': {'message': 'read failed: ' + str(e)}})
                return
            gen = _generate_thumb(image_id, src, day_str)
            if not gen:
                # 退化：直接送原图（PIL 不可用 / 异常）
                self._handle_feed_image(image_id)
                return
        try:
            with open(tp, 'rb') as f:
                data = f.read()
        except Exception:
            self._handle_feed_image(image_id)
            return
        self.send_response(200)
        self.send_header('Content-Type', 'image/jpeg')
        self.send_header('Content-Length', str(len(data)))
        # thumb 内容由 (image_id) 决定不变 → 长缓存 30 天
        self.send_header('Cache-Control', 'public, max-age=2592000, immutable')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        try:
            self.wfile.write(data)
        except Exception:
            pass

    def _handle_feed_markers(self):
        """轻量 marker 数据：所有公开图（含坐标）按 created_at DESC，封顶 limit。
        给"全球墙"tab 在地球仪上画散点用。前端拿 [{id, lat, lng}] 一次性渲染。"""
        qs = self.path.split('?', 1)[1] if '?' in self.path else ''
        params = urllib.parse.parse_qs(qs)
        try:
            limit = int((params.get('limit') or ['1000'])[0])
        except Exception:
            limit = 1000
        limit = max(1, min(limit, 2000))
        conn = _db_open()
        try:
            cur = conn.execute(
                "SELECT id, lat, lng, created_at, user_id FROM images "
                "WHERE visibility='public' AND lat IS NOT NULL AND lng IS NOT NULL "
                "ORDER BY created_at DESC LIMIT ?",
                (limit,)
            )
            rows = [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()
        out = [{
            'id': r['id'],
            'lat': r['lat'],
            'lng': r['lng'],
            'created_at': r['created_at'],
            'is_mine': r['user_id'] == self._user_id,
        } for r in rows]
        self._json_response(200, {'items': out})

    def _handle_feed_visibility(self, image_id):
        body = self._read_body(512)
        if body is None:
            self._json_response(413, {'error': {'message': 'body too large'}})
            return
        try:
            data = json.loads(body.decode('utf-8'))
        except Exception:
            self._json_response(400, {'error': {'message': 'invalid json'}})
            return
        v = data.get('visibility')
        row, status = _db_set_image_visibility(image_id, self._user_id, v)
        if status == 'not_found':
            self._json_response(404, {'error': {'message': 'image not found'}})
            return
        if status == 'forbidden':
            self._json_response(403, {'error': {'message': '不是你的图'}})
            return
        if status == 'invalid':
            self._json_response(400, {'error': {'message': 'visibility 必须是 public 或 private'}})
            return
        self._json_response(200, {'ok': True, 'visibility': row['visibility']})

    @staticmethod
    def _image_id_from_path(prefix):
        """临时静态：从 /api/feed/image/{id} 等路径里抽 image_id。"""
        return None  # 不再用；改用实例方法 _path_id 直接处理

    def _path_segment_after(self, prefix):
        """从 self.path 抽 prefix 后第一个 path 段（去掉 query）。返回 None 表示没匹配。"""
        p = self.path.split('?', 1)[0]
        if not p.startswith(prefix):
            return None
        tail = p[len(prefix):]
        seg = tail.split('/', 1)[0]
        return seg or None

    # ---------- 逆地理（Nominatim 代理）----------
    def _handle_geocode(self):
        ip = self._client_ip()
        ok, _ = _check_bucket(_geocode_buckets, ip, GEOCODE_RATE_WINDOW_SEC, GEOCODE_RATE_MAX_PER_IP)
        if not ok:
            self._json_response(429, {'error': {'message': 'geocode 限流'}})
            return

        qs = self.path.split('?', 1)[1] if '?' in self.path else ''
        params = urllib.parse.parse_qs(qs)
        lat = _to_float((params.get('lat') or [''])[0])
        lng = _to_float((params.get('lng') or [''])[0])
        if lat is None or lng is None or not (-90 <= lat <= 90) or not (-180 <= lng <= 180):
            self._json_response(400, {'error': {'message': 'lat/lng 缺失或越界'}})
            return

        # 缓存键约 1km 精度（小数点后 2 位）
        cache_key = '%.2f,%.2f' % (lat, lng)
        now = time.time()
        with _geocode_cache_lock:
            hit = _geocode_cache.get(cache_key)
            if hit and now - hit[0] < GEOCODE_CACHE_TTL_SEC:
                payload = dict(hit[1])
                payload['cached'] = True
                self._json_response(200, payload)
                return

        nm_qs = urllib.parse.urlencode({
            'lat': '%.4f' % lat,
            'lon': '%.4f' % lng,
            'format': 'jsonv2',
            'zoom': '10',
            'accept-language': 'zh-CN,zh,en',
        })
        url = 'https://nominatim.openstreetmap.org/reverse?' + nm_qs
        req = urllib.request.Request(url, headers={
            'User-Agent': 'lilibear-world/0.1 (xmu food assoc mascot; contact via repo owner)',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.5',
            'Referer': 'https://localhost/',
        })
        try:
            ctx = ssl.create_default_context()
            with urllib.request.urlopen(req, context=ctx, timeout=GEOCODE_TIMEOUT) as resp:
                raw = resp.read()
            data = json.loads(raw.decode('utf-8'))
        except urllib.error.HTTPError as e:
            self._json_response(502, {'error': {'message': 'nominatim HTTP %d' % e.code}})
            return
        except Exception as e:
            self._json_response(502, {'error': {'message': 'nominatim 失败: ' + type(e).__name__}})
            return

        addr = data.get('address') or {}
        country  = _zh_first(addr.get('country'))
        state    = _zh_first(addr.get('state') or addr.get('region'))
        # city 可能落在多种字段；按从大到小取第一个非空。
        # 海域坐标常落在 state_district/region（如 region="泉州市"）。
        city = _zh_first(addr.get('city') or addr.get('town')
                         or addr.get('county') or addr.get('village')
                         or addr.get('municipality') or addr.get('hamlet')
                         or addr.get('state_district') or addr.get('region'))
        # 区级只取 city_district/district/borough；suburb 是街道级（太细，且海外常英文）
        district = _zh_first(addr.get('city_district') or addr.get('district')
                             or addr.get('borough'))
        place_short = _build_place_short(country, state, city, district)
        full = _zh_first(data.get('display_name'))

        payload = {
            'lat': lat, 'lng': lng,
            'country': country, 'state': state, 'city': city, 'district': district,
            'place_short': place_short,
            'full': full,
            'source': 'nominatim',
            'cached': False,
        }

        with _geocode_cache_lock:
            if len(_geocode_cache) >= GEOCODE_CACHE_MAX_ENTRIES:
                # 简单清理：删 1/4 最旧条目
                items = sorted(_geocode_cache.items(), key=lambda kv: kv[1][0])
                for k, _v in items[: GEOCODE_CACHE_MAX_ENTRIES // 4]:
                    _geocode_cache.pop(k, None)
            _geocode_cache[cache_key] = (now, dict(payload))

        self._json_response(200, payload)

    # ---------- 静态 index.html ----------
    def _serve_index(self):
        path = os.path.join(DOC_ROOT, 'index.html')
        if not os.path.exists(path):
            self.send_error(404, 'index.html not found')
            return
        with open(path, 'rb') as f:
            body = f.read()
        for placeholder, value in TEMPLATE_SUBS.items():
            body = body.replace(placeholder, value)
        self.send_response(200)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.send_header('Cache-Control', 'no-cache')
        # 基础安全头
        self.send_header('X-Content-Type-Options', 'nosniff')
        self.send_header('Referrer-Policy', 'no-referrer')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    # ---------- HTTP dispatchers ----------
    def do_OPTIONS(self):
        if (self._matched_prefix()
                or self.path.startswith('/api/log/')
                or self.path.startswith('/api/geocode')
                or self.path.startswith('/api/jobs')
                or self.path.startswith('/api/me')
                or self.path.startswith('/api/feed')):
            self._cors_204()
            return
        self.send_response(204)
        self.end_headers()

    def do_GET(self):
        self._ensure_user_cookie()
        prefix = self._matched_prefix()
        if prefix:
            self._proxy(prefix)
            return
        # 拆掉 query string 再判断
        path = self.path.split('?', 1)[0]
        if path in ('/', '/index.html'):
            self._serve_index()
            return
        if path == '/api/geocode':
            self._handle_geocode()
            return
        if path == '/api/me':
            self._handle_me_get()
            return
        if path == '/api/feed':
            self._handle_feed_list()
            return
        if path == '/api/feed/markers':
            self._handle_feed_markers()
            return
        if path.startswith('/api/feed/image/'):
            seg = self._path_segment_after('/api/feed/image/')
            if seg and re.match(r'^[a-f0-9]{8,32}$', seg):
                self._handle_feed_image(seg)
            else:
                self.send_error(404, 'feed image route not found')
            return
        if path.startswith('/api/feed/thumb/'):
            seg = self._path_segment_after('/api/feed/thumb/')
            if seg and re.match(r'^[a-f0-9]{8,32}$', seg):
                self._handle_feed_thumb(seg)
            else:
                self.send_error(404, 'feed thumb route not found')
            return
        # /api/jobs/{id}  和  /api/jobs/{id}/image
        if path.startswith('/api/jobs/'):
            jid = self._job_id_from_path('/api/jobs/')
            if not jid:
                self.send_error(404, 'job route not found')
                return
            if path.endswith('/image'):
                self._handle_job_image(jid)
            else:
                self._handle_job_get(jid)
            return
        # 仅放行明确白名单的静态资源；其它一律 404，
        # 避免 .env / server.py / probe_*.py / e2e_*.bin / *.log 被下载，
        # 也避免目录列表泄漏文件结构。
        if not _is_static_allowed(path):
            self.send_error(404, 'not found')
            return
        super().do_GET()

    def do_POST(self):
        self._ensure_user_cookie()
        if self.path == '/api/log/event':
            self._handle_log_event()
            return
        if self.path == '/api/log/image':
            self._handle_log_image()
            return
        if self.path == '/api/jobs/start':
            self._handle_job_start()
            return
        if self.path.startswith('/api/jobs/') and self.path.endswith('/cancel'):
            jid = self._job_id_from_path('/api/jobs/')
            if jid:
                self._handle_job_cancel(jid)
                return
            self.send_error(404, 'job not found')
            return
        if self.path == '/api/me':
            self._handle_me_post()
            return
        # /api/feed/{image_id}/visibility
        if self.path.startswith('/api/feed/') and self.path.endswith('/visibility'):
            # 抽 image_id：去 /api/feed/ 与 /visibility
            mid = self.path[len('/api/feed/'):-len('/visibility')]
            if re.match(r'^[a-f0-9]{8,32}$', mid or ''):
                self._handle_feed_visibility(mid)
                return
            self.send_error(404, 'feed item not found')
            return
        prefix = self._matched_prefix()
        if prefix:
            self._proxy(prefix)
            return
        self.send_error(405, 'POST not allowed')

    def do_PUT(self):
        self._ensure_user_cookie()
        prefix = self._matched_prefix()
        if prefix:
            self._proxy(prefix)
        else:
            self.send_error(405)

    def do_DELETE(self):
        self._ensure_user_cookie()
        prefix = self._matched_prefix()
        if prefix:
            self._proxy(prefix)
        else:
            self.send_error(405)

    def do_PATCH(self):
        self._ensure_user_cookie()
        prefix = self._matched_prefix()
        if prefix:
            self._proxy(prefix)
        else:
            self.send_error(405)


def _to_float(s):
    try:
        return float(s)
    except Exception:
        return None


class DualStackServer(socketserver.ThreadingTCPServer):
    """同时绑 IPv4 / IPv6。"""
    daemon_threads = True
    allow_reuse_address = True
    address_family = _socket.AF_INET6

    def server_bind(self):
        with contextlib.suppress(Exception):
            self.socket.setsockopt(_socket.IPPROTO_IPV6, _socket.IPV6_V6ONLY, 0)
        return super().server_bind()


class IPv4Server(socketserver.ThreadingTCPServer):
    """仅绑 IPv4（生产环境只对 127.0.0.1 暴露 + nginx 反代时使用）。"""
    daemon_threads = True
    allow_reuse_address = True
    address_family = _socket.AF_INET


def main():
    _ensure_log_dirs()
    _db_init()
    _job_recover_on_startup()
    threading.Thread(target=_job_gc_loop, daemon=True).start()
    addr = (BIND_HOST, PORT)
    server_cls = DualStackServer if ':' in BIND_HOST else IPv4Server
    try:
        httpd = server_cls(addr, Handler)
    except OSError as e:
        if e.errno in (10048, 98):
            print('[!] Port %d 已被占用，可能服务已在运行。' % PORT)
            print('    Open: http://localhost:%d/' % PORT)
            sys.exit(1)
        raise
    print('=' * 60)
    print(' 栗栗熊环游世界 · 本地服务')
    print('=' * 60)
    print(' URL:        http://localhost:%d/' % PORT)
    print(' Proxies:')
    for p, t in PROXIES.items():
        print('   %-22s -> %s  (白名单: v1/images/edits|generations)' % (p, t))
    print(' Endpoints:')
    print('   /api/geocode?lat=&lng=    (Nominatim 代理 + 内存缓存，海外地名兜底)')
    print('   /api/jobs/start            (POST：后台跑生成，立即返 job_id)')
    print('   /api/jobs/{id}             (GET：查状态)')
    print('   /api/jobs/{id}/image       (GET：拉结果图)')
    print('   /api/jobs/{id}/cancel      (POST：用户取消)')
    print('   /api/me                    (GET/POST：当前用户 + 昵称)')
    print('   /api/feed                  (GET：全球墙分页)')
    print('   /api/feed/markers          (GET：所有公开点轻量 marker)')
    print('   /api/feed/image/{id}       (GET：feed 图片 binary，原图)')
    print('   /api/feed/thumb/{id}       (GET：feed 缩略图，长边%dpx jpeg q%d，PIL=%s)' % (
        THUMB_MAX_SIDE, THUMB_QUALITY, '✓' if _HAS_PIL else '✗ 退化原图'))
    print('   /api/feed/{id}/visibility  (POST：作者改 public/private)')
    print(' Database:')
    print('   %s' % DB_PATH)
    print(' Logs:')
    print('   events.jsonl -> %s' % EVENT_LOG_PATH)
    print('   images/      -> %s' % IMG_LOG_ROOT)
    print(' Keys:')
    print('   AMAP_KEY           : %s' % ('已注入' if AMAP_KEY else '未配置（前端会报错）'))
    print('   AMAP_SECURITY_CODE : %s' % ('已注入' if AMAP_SECURITY_CODE
                                          else '未配置（高德 JS API v2 会报 USERKEY_PLAT_NOMATCH，base tile 降级）'))
    print('   IMAGE_API_KEY      : %s' % ('已加载（仅 server 持有）' if IMAGE_API_KEY else '未配置（生成会 503）'))
    print(' Rate limit: %d 次 / %d 秒 / IP' % (RATE_MAX_PER_IP, RATE_WINDOW_SEC))
    print(' Ctrl+C 退出')
    print('=' * 60)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print('\nshutting down...')
        httpd.shutdown()


if __name__ == '__main__':
    main()
