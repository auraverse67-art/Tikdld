import threading
import time
import requests
import random
import string
import re
import sqlite3
import concurrent.futures
from dotenv import load_dotenv
import os

load_dotenv()

from flask import Flask, request, jsonify, Response
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# ===== SESSION SETUP (OPTIMIZED) =====
session = requests.Session()

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

session.headers.update({"User-Agent": "Mozilla/5.0"})

retry = Retry(
    total=3,
    backoff_factor=0.2,
    status_forcelist=[429, 500, 502, 503, 504]
)

adapter = HTTPAdapter(
    max_retries=retry,
    pool_connections=1000,
    pool_maxsize=1000
)

session.mount("http://", adapter)
session.mount("https://", adapter)

# ===== SQLITE DATABASE =====
conn = sqlite3.connect("stats.db", check_same_thread=False)
c = conn.cursor()

c.execute('''
CREATE TABLE IF NOT EXISTS stats (
    key TEXT PRIMARY KEY,
    value INTEGER
)
''')

for key in ["requests", "downloads", "cache_hits", "videos_served"]:
    c.execute("INSERT OR IGNORE INTO stats (key,value) VALUES (?,?)", (key,0))

conn.commit()

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

# ===== RAM CACHE =====
cache = {}

# ===== HELPERS =====
def clean_filename(text):
    text = re.sub(r'[\\/*?:"<>|]', "", text)
    text = re.sub(r'\s+', " ", text).strip()
    return text[:120]

def random_string(length=6):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

# ===== EXPAND SHORT TIKTOK LINKS =====
def expand_url(url):
    try:
        if "vt.tiktok.com" in url or "vm.tiktok.com" in url:
            r = session.head(url, allow_redirects=True, timeout=5)
            return r.url
    except:
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
            data = res.json().get("data", {})
            video = data.get("play")
            if video:
                return {
                    "video_url": video,
                    "title": data.get("title", ""),
                    "author": data.get("author", {}).get("nickname", "") or data.get("author", {}).get("unique_id", ""),
                    "thumbnail": data.get("cover", "")
                }
    except:
        pass
    return None


def fetch_tikwm_alt(url):
    try:
        res = session.post(
            "https://tikwm.com/api/",
            data={"url": url},
            timeout=5
        )
        if res.status_code == 200:
            data = res.json().get("data", {})
            video = data.get("play")
            if video:
                return {
                    "video_url": video,
                    "title": data.get("title", ""),
                    "author": data.get("author", {}).get("nickname", "") or data.get("author", {}).get("unique_id", ""),
                    "thumbnail": data.get("cover", "")
                }
    except:
        pass
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
            video_url = data.get("video", {}).get("no_watermark")
            if video_url:
                return {
                    "video_url": video_url,
                    "title": data.get("title", ""),
                    "author": data.get("author", ""),
                    "thumbnail": data.get("thumbnail", "")
                }
    except:
        pass
    return None

# ===== PARALLEL FETCH (ULTRA FAST) =====
def fetch_tiktok_video(url):

    url = expand_url(url)

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:

        futures = [
            executor.submit(fetch_tikwm, url),
            executor.submit(fetch_tikwm_alt, url),
            executor.submit(fetch_backup, url)
        ]

        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                result["original_url"] = url
                return result

    return None

# ===== SAVE CACHE =====
def save_cache_db(url, video_url):
    try:
        conn2 = sqlite3.connect("stats.db")
        c2 = conn2.cursor()

        c2.execute(
            "INSERT OR REPLACE INTO video_cache (url,video_url) VALUES (?,?)",
            (url,video_url)
        )

        conn2.commit()
        conn2.close()

    except Exception as e:
        print("DB thread error:", e)

# ===== DOWNLOAD ROUTE =====
@app.route("/download", methods=["POST"])
def download_video():

    try:
        data = request.get_json()
        url = data.get("url")
        ip = request.remote_addr

        if not url:
            return jsonify({"success": False, "message": "No URL"}),400

        # Stats update
        try:
            c.execute("UPDATE stats SET value=value+1 WHERE key='requests'")
            c.execute("INSERT OR IGNORE INTO unique_ips (ip) VALUES (?)",(ip,))
            conn.commit()
        except:
            pass

        # Fetch fresh data every time (no cache to avoid expired URLs)
        result = fetch_tiktok_video(url)

        print("FETCH RESULT:",result)

        if not result:
            return jsonify({"success":False,"message":"Fetch failed"}),500

        video_url = result["video_url"]
        title = result.get("title", "")
        author = result.get("author", "")
        thumbnail = result.get("thumbnail", "")
        original_url = result.get("original_url", url)

        # Save to cache (for reference, but we re-fetch on /file)
        cache[url] = video_url
        threading.Thread(
            target=save_cache_db,
            args=(url,video_url),
            daemon=True
        ).start()

        # Stats
        try:
            c.execute("UPDATE stats SET value=value+1 WHERE key='downloads'")
            c.execute("UPDATE stats SET value=value+1 WHERE key='videos_served'")
            c.execute(
                "INSERT INTO download_logs (ip,url,timestamp) VALUES (?,?,?)",
                (ip,url,int(time.time()))
            )
            conn.commit()
        except:
            pass

        filename = clean_filename(title or "ToolifyX Downloader")+"_"+random_string()+".mp4"

        return jsonify({
            "success":True,
            "url":video_url,
            "filename":filename,
            "title":title,
            "author":author,
            "thumbnail":thumbnail,
            "videoId": url
        })

    except Exception as e:
        print("CRASH PREVENTED:",e)

        return jsonify({
            "success":False,
            "message":"Server recovered automatically"
        }),500

# ===== FILE SERVING (RE-FETCH FRESH URL) =====
@app.route("/file")
def serve_file():

    # Support both old "url" param and new "videoId" param
    video_url = request.args.get("url")
    video_id = request.args.get("videoId")
    mode = request.args.get("mode","preview")

    # If videoId provided, re-fetch fresh URL
    if video_id and not video_url:
        result = fetch_tiktok_video(video_id)
        if result:
            video_url = result["video_url"]
        else:
            return jsonify({"success":False,"message":"Could not re-fetch video. Link may be expired or invalid."}),500

    if not video_url:
        return jsonify({"success":False,"message":"No video URL"}),400

    try:
        # Parse Range header from client (e.g., "bytes=0-1023" or "bytes=1024-")
        range_header = request.headers.get("Range")

        # Build request headers to forward to source
        source_headers = {}
        if range_header:
            source_headers["Range"] = range_header

        # Request from source with range support
        r = session.get(video_url, stream=True, timeout=15, headers=source_headers)

        rand = random_string()
        filename = f"ToolifyX Downloader-{rand}.mp4"

        # Determine response status
        status_code = 206 if r.status_code == 206 else 200

        # Build response headers
        headers = {
            "Content-Type": r.headers.get("Content-Type", "video/mp4"),
            "Accept-Ranges": "bytes",  # Tell client we support resume
        }

        # Forward Content-Range if source sent it (partial content)
        if "Content-Range" in r.headers:
            headers["Content-Range"] = r.headers["Content-Range"]

        # Forward Content-Length (either full or partial)
        if "Content-Length" in r.headers:
            headers["Content-Length"] = r.headers["Content-Length"]

        # Content-Disposition
        disposition = (
            f'attachment; filename="{filename}"'
            if mode=="download"
            else f'inline; filename="{filename}"'
        )
        headers["Content-Disposition"] = disposition

        # Stream generator
        def generate():
            for chunk in r.iter_content(chunk_size=65536):
                if chunk:
                    yield chunk

        return Response(
            generate(),
            status=status_code,
            headers=headers
        )

    except Exception as e:
        return jsonify({"success":False,"message":str(e)}),500

# ===== STATS =====
@app.route("/stats",methods=["GET"])
def get_stats():

    c.execute("SELECT key,value FROM stats")
    stats_data=dict(c.fetchall())

    c.execute("SELECT COUNT(*) FROM unique_ips")
    unique_ips_count=c.fetchone()[0]

    c.execute("SELECT ip,url,timestamp FROM download_logs")

    logs=[
        {"ip":ip,"url":url,"timestamp":ts}
        for ip,url,ts in c.fetchall()
    ]

    return jsonify({
        **stats_data,
        "unique_ips":unique_ips_count,
        "download_logs":logs
    })

# ===== WAKE =====
@app.route("/wake",methods=["GET"])
def wake():
    return jsonify({
        "success":True,
        "message":"Server is awake"
    })

# ===== ADMIN RESET =====
ADMIN_PASSWORD=os.getenv("ADMIN_PASSWORD")

@app.route("/admin/reset",methods=["POST"])
def reset_stats():

    data=request.get_json()
    password=data.get("password")

    if password!=ADMIN_PASSWORD:
        return jsonify({
            "success":False,
            "message":"Wrong password"
        }),401

    for key in ["requests","downloads","cache_hits","videos_served"]:
        c.execute("UPDATE stats SET value=0 WHERE key=?",(key,))

    c.execute("DELETE FROM unique_ips")
    c.execute("DELETE FROM download_logs")

    conn.commit()

    return jsonify({"success":True})

# ===== START SERVER =====
if __name__=="__main__":
    app.run(
        host="0.0.0.0",
        port=5000,
        threaded=True
    )
