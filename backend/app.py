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

# yt-dlp fallback
try:
    import yt_dlp
    YTDLP_AVAILABLE = True
except ImportError:
    YTDLP_AVAILABLE = False

app = Flask(__name__)
CORS(app)

# ===== SESSION SETUP (OPTIMIZED) =====
session = requests.Session()

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

session.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.tiktok.com/",
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

# ===== SQLITE DATABASE (WAL + THREAD LOCK) =====
DB_PATH = "stats.db"
db_lock = threading.Lock()

conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
conn.execute("PRAGMA journal_mode=WAL")
conn.execute("PRAGMA synchronous=NORMAL")
c = conn.cursor()

c.execute('''
CREATE TABLE IF NOT EXISTS stats (
    key TEXT PRIMARY KEY,
    value INTEGER
)
''')

for key in ["requests", "downloads", "cache_hits", "videos_served"]:
    c.execute("INSERT OR IGNORE INTO stats (key,value) VALUES (?,?)", (key, 0))

c.execute('''
CREATE TABLE IF NOT EXISTS unique_ips (
    ip TEXT PRIMARY KEY
)
''')

c.execute('''
CREATE TABLE IF NOT EXISTS video_cache (
    url TEXT PRIMARY KEY,
    video_url TEXT
)
''')

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
    """Thread-safe DB helper."""
    with db_lock:
        cur = conn.cursor()
        cur.execute(query, params)
        if commit:
            conn.commit()
        return cur


# ===== RAM CACHE =====
cache = {}
cache_lock = threading.Lock()

# ===== HELPERS =====
def clean_filename(text):
    if not text:
        text = "ToolifyX Downloader"
    text = re.sub(r'[\\/*?:"<>|%&]', "", text)
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
                    "thumbnail": data.get("cover", "")
                }
    except Exception as e:
        print("tikwm err:", e)
    return None


def fetch_tikwm_hd(url):
    """Same host, but requests the HD variant explicitly."""
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
                    "thumbnail": data.get("cover", "")
                }
    except Exception as e:
        print("tikwm hd err:", e)
    return None


def fetch_backup(url):
    """musicaldown alternative."""
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
                    "thumbnail": data.get("thumbnail", "")
                }
    except Exception as e:
        print("musicaldown err:", e)
    return None


def fetch_ytdlp(url):
    """
    Silent fast fallback. Extracts direct video URL with no downloads,
    minimal format probing, short socket timeout.
    """
    if not YTDLP_AVAILABLE:
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

        # Prefer direct URL
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

        return {
            "video_url": video_url,
            "title": info.get("title", "") or "",
            "author": info.get("uploader", "") or info.get("creator", "") or "",
            "thumbnail": info.get("thumbnail", "") or "",
        }

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
            fut = ex.submit(_extract)
            return fut.result(timeout=8)
    except Exception as e:
        print("yt-dlp err:", e)
        return None


# ===== PARALLEL FETCH (ULTRA FAST + SILENT YT-DLP FALLBACK) =====
def fetch_tiktok_video(url):
    url = expand_url(url)

    # Phase 1: race the three primary APIs — return on first success.
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
                # cancel remaining work
                for f in futures:
                    f.cancel()
                executor.shutdown(wait=False, cancel_futures=True)
                return result
    except concurrent.futures.TimeoutError:
        pass

    executor.shutdown(wait=False, cancel_futures=True)

    # Phase 2: silent yt-dlp fallback
    result = fetch_ytdlp(url)
    if result:
        result["original_url"] = url
        return result

    return None


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


# ===== DOWNLOAD ROUTE =====
@app.route("/download", methods=["POST"])
def download_video():
    try:
        data = request.get_json(silent=True) or {}
        url = data.get("url")
        ip = request.remote_addr

        if not url:
            return jsonify({"success": False, "message": "No URL"}), 400

        # Stats update
        try:
            db_execute("UPDATE stats SET value=value+1 WHERE key='requests'")
            db_execute("INSERT OR IGNORE INTO unique_ips (ip) VALUES (?)", (ip,))
        except Exception as e:
            print("stats err:", e)

        result = fetch_tiktok_video(url)
        print("FETCH RESULT:", result)

        if not result:
            return jsonify({"success": False, "message": "Fetch failed"}), 500

        video_url = result["video_url"]
        title = result.get("title", "")
        author = result.get("author", "")
        thumbnail = result.get("thumbnail", "")
        original_url = result.get("original_url", url)

        with cache_lock:
            cache[url] = video_url

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

        filename = clean_filename(title or "ToolifyX Downloader") + "_" + random_string() + ".mp4"

        return jsonify({
            "success": True,
            "url": video_url,
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


# ===== FILE SERVING (RE-FETCH FRESH URL) =====
@app.route("/file")
def serve_file():
    video_url = request.args.get("url")
    video_id = request.args.get("videoId")
    mode = request.args.get("mode", "preview")

    if video_id and not video_url:
        result = fetch_tiktok_video(video_id)
        if result:
            video_url = result["video_url"]
        else:
            return jsonify({
                "success": False,
                "message": "Could not re-fetch video. Link may be expired or invalid."
            }), 500

    if not video_url:
        return jsonify({"success": False, "message": "No video URL"}), 400

    try:
        range_header = request.headers.get("Range")

        source_headers = {
            "Referer": "https://www.tiktok.com/",
            "User-Agent": session.headers["User-Agent"],
            "Accept": "*/*",
        }
        if range_header:
            source_headers["Range"] = range_header

        r = session.get(video_url, stream=True, timeout=15, headers=source_headers)

        # Guard: never stream upstream errors as video
        if r.status_code >= 400:
            r.close()
            return jsonify({
                "success": False,
                "message": f"Upstream source returned {r.status_code}"
            }), 502

        rand = random_string()
        base_name = "ToolifyX Downloader"
        ascii_filename = f"{base_name}-{rand}.mp4"
        utf8_filename = quote(f"{base_name}-{rand}.mp4")

        status_code = 206 if r.status_code == 206 else 200

        headers = {
            "Content-Type": r.headers.get("Content-Type", "video/mp4"),
            "Accept-Ranges": "bytes",
        }

        if "Content-Range" in r.headers:
            headers["Content-Range"] = r.headers["Content-Range"]

        if "Content-Length" in r.headers:
            headers["Content-Length"] = r.headers["Content-Length"]

        disposition = "attachment" if mode == "download" else "inline"
        headers["Content-Disposition"] = (
            f"{disposition}; filename=\"{ascii_filename}\"; filename*=UTF-8''{utf8_filename}"
        )

        def generate():
            try:
                for chunk in r.iter_content(chunk_size=65536):
                    if chunk:
                        yield chunk
            finally:
                r.close()

        return Response(generate(), status=status_code, headers=headers)

    except Exception as e:
        print("file err:", e)
        return jsonify({"success": False, "message": str(e)}), 500


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

    logs = [
        {"ip": ip, "url": url, "timestamp": ts}
        for ip, url, ts in rows
    ]

    return jsonify({
        **stats_data,
        "unique_ips": unique_ips_count,
        "download_logs": logs
    })


# ===== WAKE =====
@app.route("/wake", methods=["GET"])
def wake():
    return jsonify({
        "success": True,
        "message": "Server is awake"
    })


# ===== ADMIN RESET =====
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD")


@app.route("/admin/reset", methods=["POST"])
def reset_stats():
    data = request.get_json(silent=True) or {}
    password = data.get("password")

    if not ADMIN_PASSWORD or password != ADMIN_PASSWORD:
        return jsonify({
            "success": False,
            "message": "Wrong password"
        }), 401

    for key in ["requests", "downloads", "cache_hits", "videos_served"]:
        db_execute("UPDATE stats SET value=0 WHERE key=?", (key,))

    db_execute("DELETE FROM unique_ips")
    db_execute("DELETE FROM download_logs")

    return jsonify({"success": True})


# ===== START SERVER =====
if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=5000,
        threaded=True
)
