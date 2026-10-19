import threading
import time
import requests
import random
import string
import re
import sqlite3
import concurrent.futures
import os
from urllib.parse import quote

from dotenv import load_dotenv
load_dotenv()

from flask import Flask, request, jsonify, Response, redirect
from flask_cors import CORS

# curl_cffi — TLS impersonation
try:
    from curl_cffi import requests as cffi_requests
    CURL_CFFI_AVAILABLE = True
except ImportError:
    cffi_requests = None
    CURL_CFFI_AVAILABLE = False
    print("WARN: curl_cffi not installed.")

YTDLP_AVAILABLE = None

app = Flask(__name__)
CORS(app)

# ===== SESSION SETUP =====
session = requests.Session()
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "*/*",
})

retry = Retry(
    total=3,
    backoff_factor=0.2,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=frozenset(["GET", "POST", "HEAD"])
)
adapter = HTTPAdapter(max_retries=retry, pool_connections=100, pool_maxsize=100)
session.mount("http://", adapter)
session.mount("https://", adapter)

# ===== CDN PROXY & IMPERSONATION =====
CDN_PROXY = os.getenv("CDN_PROXY") 
CDN_IMPERSONATE = os.getenv("CDN_IMPERSONATE", "chrome124")

print(f"CDN_PROXY={'set' if CDN_PROXY else 'NOT SET'}")
print(f"CURL_CFFI_AVAILABLE={CURL_CFFI_AVAILABLE}")

# ===== SQLITE DATABASE =====
DB_PATH = os.getenv("DB_PATH", "stats.db")
db_lock = threading.Lock()

conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
conn.execute("PRAGMA journal_mode=WAL")
c = conn.cursor()

c.execute('CREATE TABLE IF NOT EXISTS stats (key TEXT PRIMARY KEY, value INTEGER)')
for key in ["requests", "downloads", "cache_hits", "videos_served"]:
    c.execute("INSERT OR IGNORE INTO stats (key,value) VALUES (?,?)", (key, 0))
c.execute('CREATE TABLE IF NOT EXISTS unique_ips (ip TEXT PRIMARY KEY)')
c.execute('CREATE TABLE IF NOT EXISTS video_cache (url TEXT PRIMARY KEY, video_url TEXT)')
c.execute('CREATE TABLE IF NOT EXISTS download_logs (id INTEGER PRIMARY KEY AUTOINCREMENT, ip TEXT, url TEXT, timestamp INTEGER)')
conn.commit()

def db_execute(query, params=(), commit=True):
    with db_lock:
        cur = conn.cursor()
        cur.execute(query, params)
        if commit: conn.commit()
        return cur

# ===== HELPERS =====
def clean_filename(text):
    if not text: text = "ToolifyX Downloader"
    text = re.sub(r'[\\/*?:"<>|%&\r\n]', "", text)
    return re.sub(r'\s+', " ", text).strip()[:120] or "ToolifyX Downloader"

def random_string(length=6):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def expand_url(url):
    try:
        if "vt.tiktok.com" in url or "vm.tiktok.com" in url:
            r = session.get(url, allow_redirects=True, timeout=5, stream=True)
            final = r.url
            r.close()
            return final
    except Exception: pass
    return url

# ===== FETCHERS =====
def fetch_tikwm(url):
    try:
        res = session.post("https://www.tikwm.com/api/", data={"url": url, "hd": "1"}, timeout=5)
        if res.status_code == 200:
            data = res.json().get("data") or {}
            video = data.get("play")
            if video and isinstance(video, str) and video.startswith("http"):
                return {
                    "video_url": video,
                    "title": data.get("title", ""),
                    "author": (data.get("author", {}) or {}).get("nickname", ""),
                    "thumbnail": data.get("cover", ""),
                    "http_headers": {}
                }
    except Exception as e: print("tikwm err:", e)
    return None

def fetch_tikwm_hd(url):
    try:
        res = session.post("https://www.tikwm.com/api/", data={"url": url, "hd": "1", "wm": "0"}, timeout=5)
        if res.status_code == 200:
            data = res.json().get("data") or {}
            video = data.get("hdplay") or data.get("play")
            if video and isinstance(video, str) and video.startswith("http"):
                return {
                    "video_url": video,
                    "title": data.get("title", ""),
                    "author": (data.get("author", {}) or {}).get("nickname", ""),
                    "thumbnail": data.get("cover", ""),
                    "http_headers": {}
                }
    except Exception as e: print("tikwm hd err:", e)
    return None

def fetch_ytdlp(url):
    global YTDLP_AVAILABLE
    if YTDLP_AVAILABLE is False: return None
    try:
        import yt_dlp
        YTDLP_AVAILABLE = True
    except ImportError:
        YTDLP_AVAILABLE = False
        return None

    def _extract():
        ydl_opts = {"quiet": True, "no_warnings": True, "skip_download": True, "noplaylist": True, "format": "mp4/best[ext=mp4]/best", "socket_timeout": 6}
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
        if not info: return None
        video_url = info.get("url")
        if not video_url:
            for f in info.get("formats") or []:
                if f.get("url") and f.get("ext") == "mp4":
                    video_url = f["url"]
                    break
        if not video_url or not video_url.startswith("http"): return None
        
        http_headers = info.get("http_headers") or {}
        http_headers.pop("Accept-Encoding", None)
        return {
            "video_url": video_url,
            "title": info.get("title", "") or "",
            "author": info.get("uploader", "") or info.get("creator", "") or "",
            "thumbnail": info.get("thumbnail", "") or "",
            "http_headers": http_headers
        }
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
            return ex.submit(_extract).result(timeout=8)
    except Exception as e:
        print("yt-dlp err:", e)
        return None

def fetch_tiktok_video(url):
    url = expand_url(url)
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
    futures = [executor.submit(fetch_tikwm, url), executor.submit(fetch_tikwm_hd, url)]
    try:
        for future in concurrent.futures.as_completed(futures, timeout=7):
            result = future.result()
            if result:
                result["original_url"] = url
                for f in futures: f.cancel()
                executor.shutdown(wait=False, cancel_futures=True)
                return result
    except Exception: pass
    executor.shutdown(wait=False, cancel_futures=True)
    result = fetch_ytdlp(url)
    if result:
        result["original_url"] = url
        return result
    return None

# ===== DOWNLOAD ROUTE (Returns Direct URL) =====
@app.route("/download", methods=["POST"])
def download_video():
    try:
        data = request.get_json(silent=True) or {}
        url = data.get("url")
        ip = request.remote_addr

        if not url: return jsonify({"success": False, "message": "No URL"}), 400

        try:
            db_execute("UPDATE stats SET value=value+1 WHERE key='requests'")
            db_execute("INSERT OR IGNORE INTO unique_ips (ip) VALUES (?)", (ip,))
        except: pass

        result = fetch_tiktok_video(url)
        if not result:
            return jsonify({"success": False, "message": "Fetch failed"}), 500

        video_url = result["video_url"]
        title = result.get("title", "")
        filename = clean_filename(title) + "_" + random_string() + ".mp4"

        try:
            db_execute("UPDATE stats SET value=value+1 WHERE key='downloads'")
            db_execute("INSERT INTO download_logs (ip,url,timestamp) VALUES (?,?,?)", (ip, url, int(time.time())))
        except: pass

        return jsonify({
            "success": True,
            "url": video_url,
            "filename": filename,
            "title": title,
            "author": result.get("author", ""),
            "thumbnail": result.get("thumbnail", ""),
            "videoId": url
        })
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

# ===== DIRECT DOWNLOAD REDIRECT =====
@app.route("/direct-download")
def direct_download():
    target_url = request.args.get("url")
    if not target_url: return "Missing URL", 400

    # 302 Redirect forces the browser to follow the link and download from the source (TikTok)
    return redirect(target_url, code=302)

@app.route("/wake", methods=["GET"])
def wake():
    return jsonify({"success": True})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
