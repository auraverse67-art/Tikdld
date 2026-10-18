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

from flask import Flask, request, jsonify, Response
from flask_cors import CORS

# curl_cffi (kept just in case we need it later)
try:
    from curl_cffi import requests as cffi_requests
    CURL_CFFI_AVAILABLE = True
except ImportError:
    CURL_CFFI_AVAILABLE = False

YTDLP_AVAILABLE = None 

app = Flask(__name__)
CORS(app)

# ===== SESSION =====
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "*/*",
})

# ===== DB =====
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
    except Exception:
        pass
    return url

# ===== FETCHERS =====
def fetch_tikwm(url):
    try:
        res = session.post("https://www.tikwm.com/api/", data={"url": url, "hd": "1"}, timeout=5)
        if res.status_code == 200:
            data = res.json().get("data") or {}
            video = data.get("play")
            if video:
                return {"video_url": video, "title": data.get("title", ""), "author": data.get("author", {}).get("nickname", ""), "thumbnail": data.get("cover", "")}
    except: pass
    return None

def fetch_tiktok_video(url):
    url = expand_url(url)
    return fetch_tikwm(url)

# ===== DOWNLOAD ROUTE (Returns Direct URL & Filename) =====
@app.route("/download", methods=["POST"])
def download_video():
    try:
        data = request.get_json(silent=True) or {}
        url = data.get("url")
        if not url: return jsonify({"success": False, "message": "No URL"}), 400

        result = fetch_tiktok_video(url)
        if not result: return jsonify({"success": False, "message": "Fetch failed"}), 500

        filename = clean_filename(result.get("title", "")) + "_" + random_string() + ".mp4"

        return jsonify({
            "success": True,
            "url": result["video_url"], # Direct TikTok CDN link
            "filename": filename,
            "title": result.get("title", ""),
            "author": result.get("author", ""),
            "thumbnail": result.get("thumbnail", ""),
            "videoId": url
        })
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

# ===== THE MAGIC DOWNLOAD ROUTE =====
# This route downloads the file on the server and forces the browser to save it.
@app.route("/force-download")
def force_download():
    target_url = request.args.get("url")
    filename = request.args.get("filename", "video.mp4")

    if not target_url:
        return "Missing URL", 400

    # Clean filename
    filename = re.sub(r'[\\/*?:"<>|\r\n]', "", filename)

    def generate():
        try:
            # Fetch directly from TikTok
            r = session.get(target_url, stream=True, timeout=20)
            for chunk in r.iter_content(chunk_size=65536):
                if chunk: yield chunk
        except Exception as e:
            print("Stream error:", e)

    headers = {
        "Content-Type": "video/mp4",
        "Content-Disposition": f'attachment; filename="{filename}"'
    }
    return Response(generate(), headers=headers)

@app.route("/wake", methods=["GET"])
def wake():
    return jsonify({"success": True})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
