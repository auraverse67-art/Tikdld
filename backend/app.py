import threading
import time
import requests
import random
import string
import re
import sqlite3
import concurrent.futures
import os
import json
from urllib.parse import quote

from dotenv import load_dotenv
load_dotenv()

from flask import Flask, request, jsonify, Response
from flask_cors import CORS

# curl_cffi — TLS impersonation. Optional but REQUIRED to beat TikTok's
# TLS fingerprinting on datacenter IPs. Falls back to requests if missing.
try:
    from curl_cffi import requests as cffi_requests
    CURL_CFFI_AVAILABLE = True
except ImportError:
    cffi_requests = None
    CURL_CFFI_AVAILABLE = False
    print("WARN: curl_cffi not installed — CDN streaming may 403. "
          "Run: pip install curl_cffi")

YTDLP_AVAILABLE = None  # resolved on first use


app = Flask(__name__)
CORS(app)

# ===== SESSION SETUP =====
session = requests.Session()

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

session.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
})

retry = Retry(
    total=3,
    backoff_factor=0.2,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=frozenset(["GET", "POST", "HEAD"])
)

adapter = HTTPAdapter(
    max_retries=retry,
    pool_connections=1000,
    pool_maxsize=1000
)

session.mount("http://", adapter)
session.mount("https://", adapter)

# ===== CDN PROXY & IMPERSONATION CONFIG =====
CDN_PROXY = os.getenv("CDN_PROXY")           # e.g. https://your-worker.workers.dev
CDN_IMPERSONATE = os.getenv("CDN_IMPERSONATE", "chrome124")

print(f"CDN_PROXY={'set' if CDN_PROXY else 'NOT SET'}")
print(f"CDN_IMPERSONATE={CDN_IMPERSONATE}")
print(f"CURL_CFFI_AVAILABLE={CURL_CFFI_AVAILABLE}")

# ===== SQLITE DATABASE =====
DB_PATH = os.getenv("DB_PATH", "stats.db")
db_lock = threading.Lock()

conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
conn.execute("PRAGMA journal_mode=WAL")
conn.execute("PRAGMA synchronous=NORMAL")
c = conn.cursor()

c.execute('''CREATE TABLE IF NOT EXISTS stats (key TEXT PRIMARY KEY, value INTEGER)''')
for key in ["requests", "downloads", "cache_hits", "videos_served"]:
    c.execute("INSERT OR IGNORE INTO stats (key,value) VALUES (?,?)", (key, 0))

c.execute('''CREATE TABLE IF NOT EXISTS unique_ips (ip TEXT PRIMARY KEY)''')
c.execute('''CREATE TABLE IF NOT EXISTS video_cache (url TEXT PRIMARY KEY, video_url TEXT)''')
c.execute('''
CREATE TABLE IF NOT EXISTS download_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ip TEXT,
    url TEXT,
    timestamp INTEGER
)
''')
conn.commit()


def db_execute(query, params=(), commit=True):
    with db_lock:
        cur = conn.cursor()
        cur.execute(query, params)
        if commit:
            conn.commit()
        return cur


# ===== IN-MEMORY URL CACHE =====
url_cache = {}
url_cache_lock = threading.Lock()
URL_TTL = 60   # reduced — TikTok signed URLs often live <90s


# ===== HELPERS =====
def clean_filename(text):
    if not text:
        text = "ToolifyX Downloader"
    text = re.sub(r'[\\/*?:"<>|%&\r\n]', "", text)
    text = re.sub(r'\s+', " ", text).strip()
    return text[:120] or "ToolifyX Downloader"


def random_string(length=6):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


# ===== EXPAND SHORT TIKTOK LINKS =====
def expand_url(url):
    try:
        if "vt.tiktok.com" in url or "vm.tiktok.com" in url:
            r = session.get(url, allow_redirects=True, timeout=5, stream=True)
            final = r.url
            r.close()
            return final
    except Exception:
        pass
    return url


# ===== API FETCHERS =====
def fetch_tikwm(url):
    try:
        res = session.post(
            "https://www.tikwm.com/api/",
            data={"url": url, "hd": "1"},
            timeout=5
        )
        if res.status_code == 200:
            data = res.json().get("data") or {}
            video = data.get("play")
            if video and isinstance(video, str) and video.startswith("http"):
                return {
                    "video_url": video,
                    "title": data.get("title", ""),
                    "author": (data.get("author", {}) or {}).get("nickname", "")
                              or (data.get("author", {}) or {}).get("unique_id", ""),
                    "thumbnail": data.get("cover", ""),
                    "http_headers": {},
                }
    except Exception as e:
        print("tikwm err:", e)
    return None


def fetch_tikwm_hd(url):
    try:
        res = session.post(
            "https://www.tikwm.com/api/",
            data={"url": url, "hd": "1", "wm": "0"},
            timeout=5
        )
        if res.status_code == 200:
            data = res.json().get("data") or {}
            video = data.get("hdplay") or data.get("play")
            if video and isinstance(video, str) and video.startswith("http"):
                return {
                    "video_url": video,
                    "title": data.get("title", ""),
                    "author": (data.get("author", {}) or {}).get("nickname", "")
                              or (data.get("author", {}) or {}).get("unique_id", ""),
                    "thumbnail": data.get("cover", ""),
                    "http_headers": {},
                }
    except Exception as e:
        print("tikwm hd err:", e)
    return None


def fetch_backup(url):
    try:
        res = session.post(
            "https://api2.musicaldown.com/v2/download",
            data={"url": url},
            timeout=6
        )
        if res.status_code == 200:
            data = res.json()
            video_url = (data.get("video") or {}).get("no_watermark")
            if video_url and video_url.startswith("http"):
                return {
                    "video_url": video_url,
                    "title": data.get("title", ""),
                    "author": data.get("author", ""),
                    "thumbnail": data.get("thumbnail", ""),
                    "http_headers": {},
                }
    except Exception as e:
        print("musicaldown err:", e)
    return None


def fetch_ytdlp(url):
    global YTDLP_AVAILABLE
    if YTDLP_AVAILABLE is False:
        return None

    try:
        import yt_dlp  # noqa
        YTDLP_AVAILABLE = True
    except ImportError:
        YTDLP_AVAILABLE = False
        return None

    def _extract():
        ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            "skip_download": True,
            "noplaylist": True,
            "format": "mp4/best[ext=mp4]/best",
            "socket_timeout": 6,
            "extract_flat": False,
            "check_formats": False,
            "nocheckcertificate": True,
            "geo_bypass": True,
        }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)

        if not info:
            return None

        video_url = info.get("url")
        if not video_url:
            for f in info.get("formats") or []:
                if f.get("url") and f.get("ext") == "mp4":
                    video_url = f["url"]
                    break
            if not video_url:
                for f in info.get("formats") or []:
                    if f.get("url"):
                        video_url = f["url"]
                        break

        if not video_url or not video_url.startswith("http"):
            return None

        http_headers = info.get("http_headers") or {}
        http_headers.pop("Accept-Encoding", None)

        return {
            "video_url": video_url,
            "title": info.get("title", "") or "",
            "author": info.get("uploader", "") or info.get("creator", "") or "",
            "thumbnail": info.get("thumbnail", "") or "",
            "http_headers": http_headers,
        }

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
            fut = ex.submit(_extract)
            return fut.result(timeout=8)
    except Exception as e:
        print("yt-dlp err:", e)
        return None


# ===== PARALLEL FETCH =====
def fetch_tiktok_video(url):
    url = expand_url(url)

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=3)
    futures = [
        executor.submit(fetch_tikwm, url),
        executor.submit(fetch_tikwm_hd, url),
        executor.submit(fetch_backup, url),
    ]

    try:
        for future in concurrent.futures.as_completed(futures, timeout=7):
            try:
                result = future.result()
            except Exception:
                result = None
            if result:
                result["original_url"] = url
                for f in futures:
                    f.cancel()
                executor.shutdown(wait=False, cancel_futures=True)
                return result
    except concurrent.futures.TimeoutError:
        pass

    executor.shutdown(wait=False, cancel_futures=True)

    result = fetch_ytdlp(url)
    if result:
        result["original_url"] = url
        return result

    return None


# ===== FRESH-URL CACHE =====
def get_fresh_video(video_id):
    now = time.time()
    with url_cache_lock:
        cached = url_cache.get(video_id)
        if cached and cached["expires_at"] > now:
            return cached["result"], True

    result = fetch_tiktok_video(video_id)
    if not result:
        return None, False

    with url_cache_lock:
        url_cache[video_id] = {
            "result": result,
            "expires_at": now + URL_TTL,
        }

    return result, False


# ===== SAVE CACHE =====
def save_cache_db(url, video_url):
    try:
        conn2 = sqlite3.connect(DB_PATH, timeout=30)
        conn2.execute("PRAGMA journal_mode=WAL")
        c2 = conn2.cursor()
        c2.execute(
            "INSERT OR REPLACE INTO video_cache (url,video_url) VALUES (?,?)",
            (url, video_url)
        )
        conn2.commit()
        conn2.close()
    except Exception as e:
        print("DB thread error:", e)


# ===== STREAM HEADER BUILDER =====
def _build_stream_headers(video_url, referer, extra_headers, range_header=None):
    h = {
        "User-Agent": session.headers["User-Agent"],
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
    }

    if extra_headers:
        for k, v in extra_headers.items():
            if v:
                h[k] = v

    if "Referer" not in h and referer:
        h["Referer"] = referer
        h.setdefault("Origin", "https://www.tiktok.com")

    if range_header:
        h["Range"] = range_header

    return h


# ===== UPSTREAM OPENER (curl_cffi + proxy aware) =====
def _target_url(video_url):
    """Route through CDN proxy if configured."""
    if CDN_PROXY:
        return f"{CDN_PROXY}?u={quote(video_url, safe='')}"
    return video_url


def _fetch_stream(url, headers, timeout=20):
    """
    Get a streaming response from a TikTok CDN URL.
    Uses curl_cffi (with browser TLS fingerprint) if available,
    otherwise falls back to requests.
    """
    target = _target_url(url)

    if CURL_CFFI_AVAILABLE:
        try:
            r = cffi_requests.get(
                target,
                headers=headers,
                impersonate=CDN_IMPERSONATE,
                stream=True,
                timeout=timeout,
                allow_redirects=True,
            )
            return r, "curl_cffi"
        except Exception as e:
            print(f"curl_cffi err: {e}")

    # fallback
    r = session.get(target, stream=True, timeout=timeout, headers=headers)
    return r, "requests"


def _open_upstream(video_url, referer, extra_headers, range_header):
    """
    Try multiple header profiles until one returns < 400.
    Returns (response, profile_name) or (None, last_status).
    """
    profiles = [
        ("primary",    _build_stream_headers(video_url, referer, extra_headers, range_header)),
        ("no-referer", _build_stream_headers(video_url, None,    extra_headers, range_header)),
    ]

    mobile = {
        "User-Agent": (
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 "
            "Mobile/15E148 Safari/604.1"
        ),
        "Accept": "*/*",
    }
    if extra_headers:
        for k, v in extra_headers.items():
            if v:
                mobile.setdefault(k, v)
    if range_header:
        mobile["Range"] = range_header
    profiles.append(("mobile-ua", mobile))

    last_status = None
    for name, hdrs in profiles:
        try:
            r, engine = _fetch_stream(video_url, hdrs)
            if r.status_code < 400:
                return r, f"{name}+{engine}"
            last_status = f"{r.status_code}({name}+{engine})"
            r.close()
        except Exception as e:
            last_status = f"exc:{e}"
            continue

    return None, last_status


# ===== DOWNLOAD ROUTE =====
@app.route("/download", methods=["POST"])
def download_video():
    try:
        data = request.get_json(silent=True) or {}
        url = data.get("url")
        ip = request.remote_addr

        if not url:
            return jsonify({"success": False, "message": "No URL"}), 400

        try:
            db_execute("UPDATE stats SET value=value+1 WHERE key='requests'")
            db_execute("INSERT OR IGNORE INTO unique_ips (ip) VALUES (?)", (ip,))
        except Exception as e:
            print("stats err:", e)

        result = fetch_tiktok_video(url)
        print("FETCH RESULT:", {k: v for k, v in (result or {}).items() if k != "http_headers"})

        if not result:
            return jsonify({"success": False, "message": "Fetch failed"}), 500

        video_url = result["video_url"]
        title = result.get("title", "")
        author = result.get("author", "")
        thumbnail = result.get("thumbnail", "")

        with url_cache_lock:
            url_cache[url] = {
                "result": result,
                "expires_at": time.time() + URL_TTL,
            }

        threading.Thread(
            target=save_cache_db,
            args=(url, video_url),
            daemon=True
        ).start()

        try:
            db_execute("UPDATE stats SET value=value+1 WHERE key='downloads'")
            db_execute("UPDATE stats SET value=value+1 WHERE key='videos_served'")
            db_execute(
                "INSERT INTO download_logs (ip,url,timestamp) VALUES (?,?,?)",
                (ip, url, int(time.time()))
            )
        except Exception as e:
            print("stats err:", e)

        # ... inside @app.route("/download") ...

filename = clean_filename(title or "ToolifyX Downloader") + "_" + random_string() + ".mp4"

return jsonify({
    "success": True,
    "url": f"{CDN_PROXY}?u={quote(video_url, safe='')}",  # <--- CHANGED THIS LINE
    "filename": filename,
    "title": title,
    "author": author,
    "thumbnail": thumbnail,
    "videoId": url
})

    except Exception as e:
        print("CRASH PREVENTED:", e)
        return jsonify({
            "success": False,
            "message": "Server recovered automatically"
        }), 500


# ===== FILE SERVING =====
@app.route("/file")
def serve_file():
    video_url = request.args.get("url")
    video_id = request.args.get("videoId")
    mode = request.args.get("mode", "preview")
    custom_filename = request.args.get("filename")

    referer = None
    extra_headers = {}

    if not video_url and video_id:
        cached_result, from_cache = get_fresh_video(video_id)
        if not cached_result:
            return jsonify({
                "success": False,
                "message": "Could not re-fetch video. Link may be expired or invalid."
            }), 500
        video_url = cached_result["video_url"]
        referer = cached_result.get("original_url") or video_id
        extra_headers = cached_result.get("http_headers") or {}

        if from_cache:
            try:
                db_execute("UPDATE stats SET value=value+1 WHERE key='cache_hits'")
            except Exception:
                pass
    elif video_id:
        referer = video_id

    if not video_url:
        return jsonify({"success": False, "message": "No video URL"}), 400

    range_header = request.headers.get("Range")

    r, profile = _open_upstream(video_url, referer, extra_headers, range_header)
    if r is None:
        print(f"upstream fail url={video_url[:100]} profile={profile}")
        return jsonify({
            "success": False,
            "message": f"Upstream source returned {profile}"
        }), 502

    if custom_filename:
        custom_filename = re.sub(r'[\\/*?:"<>|\r\n]', "", custom_filename)[:120]
        if not custom_filename.lower().endswith(".mp4"):
            custom_filename += ".mp4"
        ascii_filename = custom_filename
        utf8_filename = quote(custom_filename)
    else:
        rand = random_string()
        ascii_filename = f"ToolifyX Downloader-{rand}.mp4"
        utf8_filename = quote(ascii_filename)

    status_code = 206 if r.status_code == 206 else 200

    headers = {
        "Content-Type": r.headers.get("Content-Type", "video/mp4"),
        "Accept-Ranges": "bytes",
        "Cache-Control": "no-store",
    }
    if "Content-Range" in r.headers:
        headers["Content-Range"] = r.headers["Content-Range"]
    if "Content-Length" in r.headers:
        headers["Content-Length"] = r.headers["Content-Length"]

    disposition = "attachment" if mode == "download" else "inline"
    headers["Content-Disposition"] = (
        f'{disposition}; filename="{ascii_filename}"; '
        f"filename*=UTF-8''{utf8_filename}"
    )

    def generate():
        try:
            for chunk in r.iter_content(chunk_size=65536):
                if chunk:
                    yield chunk
        finally:
            try:
                r.close()
            except Exception:
                pass

    return Response(generate(), status=status_code, headers=headers)


# ===== STATS =====
@app.route("/stats", methods=["GET"])
def get_stats():
    with db_lock:
        cur = conn.cursor()
        cur.execute("SELECT key,value FROM stats")
        stats_data = dict(cur.fetchall())
        cur.execute("SELECT COUNT(*) FROM unique_ips")
        unique_ips_count = cur.fetchone()[0]
        cur.execute("SELECT ip,url,timestamp FROM download_logs")
        rows = cur.fetchall()

    logs = [{"ip": ip, "url": url, "timestamp": ts} for ip, url, ts in rows]

    return jsonify({
        **stats_data,
        "unique_ips": unique_ips_count,
        "download_logs": logs
    })


# ===== WAKE =====
@app.route("/wake", methods=["GET"])
def wake():
    return jsonify({"success": True, "message": "Server is awake"})


# ===== DEBUG =====
@app.route("/debug-file")
def debug_file():
    video_id = request.args.get("videoId")
    if not video_id:
        return jsonify({"error": "no videoId"}), 400

    result = fetch_tiktok_video(video_id)
    if not result:
        return jsonify({"step": "fetch", "error": "fetch failed"}), 500

    video_url = result["video_url"]
    referer = result.get("original_url") or video_id
    extra_headers = result.get("http_headers") or {}

    attempts = []
    for name, hdrs in [
        ("primary",    _build_stream_headers(video_url, referer, extra_headers)),
        ("no-referer", _build_stream_headers(video_url, None,    extra_headers)),
    ]:
        try:
            r, engine = _fetch_stream(video_url, hdrs, timeout=15)
            body_sample = b""
            if r.status_code >= 400:
                try:
                    body_sample = b"".join(r.iter_content(128))[:200]
                except Exception:
                    pass
            attempts.append({
                "profile": name,
                "engine": engine,
                "status": r.status_code,
                "content_type": r.headers.get("Content-Type"),
                "content_length": r.headers.get("Content-Length"),
                "body_sample": body_sample.decode("utf-8", "replace") if body_sample else None,
            })
            r.close()
        except Exception as e:
            attempts.append({"profile": name, "error": str(e)})

    return jsonify({
        "video_url_host": video_url.split("/")[2] if "//" in video_url else "?",
        "video_url_prefix": video_url[:150],
        "had_ytdlp_headers": bool(extra_headers),
        "curl_cffi": CURL_CFFI_AVAILABLE,
        "cdn_proxy_set": bool(CDN_PROXY),
        "impersonate": CDN_IMPERSONATE,
        "attempts": attempts,
    })


@app.route("/debug-fresh")
def debug_fresh():
    """Test if URL works IMMEDIATELY after extraction (isolates expiry issue)."""
    video_id = request.args.get("videoId")
    if not video_id:
        return jsonify({"error": "no videoId"}), 400

    t0 = time.time()
    result = fetch_tiktok_video(video_id)
    if not result:
        return jsonify({"step": "fetch", "error": "fetch failed"}), 500
    fetch_ms = int((time.time() - t0) * 1000)

    video_url = result["video_url"]
    referer = result.get("original_url") or video_id
    extra_headers = result.get("http_headers") or {}
    hdrs = _build_stream_headers(video_url, referer, extra_headers)

    t1 = time.time()
    try:
        r, engine = _fetch_stream(video_url, hdrs, timeout=15)
        status = r.status_code
        ct = r.headers.get("Content-Type")
        cl = r.headers.get("Content-Length")
        body_sample = b""
        if status >= 400:
            try:
                body_sample = b"".join(r.iter_content(128))[:200]
            except Exception:
                pass
        r.close()
    except Exception as e:
        return jsonify({
            "fetch_ms": fetch_ms,
            "stream_error": str(e),
            "video_url_prefix": video_url[:150],
        })
    stream_ms = int((time.time() - t1) * 1000)

    return jsonify({
        "fetch_ms": fetch_ms,
        "stream_ms": stream_ms,
        "engine": engine,
        "status": status,
        "content_type": ct,
        "content_length": cl,
        "body_sample": body_sample.decode("utf-8", "replace") if body_sample else None,
        "video_url_prefix": video_url[:150],
        "url_has_expire": "expire=" in video_url,
    })


# ===== ADMIN RESET =====
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD")


@app.route("/admin/reset", methods=["POST"])
def reset_stats():
    data = request.get_json(silent=True) or {}
    password = data.get("password")

    if not ADMIN_PASSWORD or password != ADMIN_PASSWORD:
        return jsonify({"success": False, "message": "Wrong password"}), 401

    for key in ["requests", "downloads", "cache_hits", "videos_served"]:
        db_execute("UPDATE stats SET value=0 WHERE key=?", (key,))

    db_execute("DELETE FROM unique_ips")
    db_execute("DELETE FROM download_logs")

    with url_cache_lock:
        url_cache.clear()

    return jsonify({"success": True})


# ===== START SERVER =====
if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", 5000)),
        threaded=True
                )
