#!/usr/bin/env python3
"""
Downly Worker - generic webpage media scanner/downloader.

No YouTube-specific extraction is used.

API:
  GET  /health
  POST /v1/crawl        scan a public webpage for media URLs
  POST /v1/stream       redirect/proxy a direct media URL for preview
  POST /v1/jobs         download a detected media URL
  GET  /v1/jobs/:id
  GET  /v1/jobs/:id/file
  POST /v1/jobs/:id/cancel
"""
import html
import json
import mimetypes
import os
import re
import socket
import ssl
import shutil
import subprocess
import threading
import time
import uuid
from html.parser import HTMLParser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urljoin, urlparse, unquote
from urllib.request import Request, urlopen, HTTPRedirectHandler, build_opener

TOKEN = os.environ.get("WORKER_TOKEN", "")
HOST = os.environ.get("HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", "8080"))
ROOT = Path(os.environ.get("DOWNLOAD_DIR", str(Path.home() / "downly-worker" / "downloads"))).expanduser()
MAX_JOBS = max(1, int(os.environ.get("MAX_JOBS", "2")))
JOB_TTL = max(300, int(os.environ.get("JOB_TTL_SECONDS", "86400")))
MAX_HTML = 8 * 1024 * 1024
ROOT.mkdir(parents=True, exist_ok=True)

MEDIA_EXTS = {
    "mp4": "video/mp4", "webm": "video/webm", "mov": "video/quicktime",
    "m4v": "video/x-m4v", "avi": "video/x-msvideo", "mpg": "video/mpeg",
    "mpeg": "video/mpeg", "mkv": "video/x-matroska", "flv": "video/x-flv",
    "ogv": "video/ogg", "3gp": "video/3gpp", "m3u8": "application/vnd.apple.mpegurl",
    "mpd": "application/dash+xml",
}
MEDIA_MIME_RE = re.compile(r"^(video|application/vnd.apple.mpegurl|application/x-mpegurl|application/dash\+xml)/", re.I)
URL_RE = re.compile(r'''https?://[^\s\"'<>]+''', re.I)

jobs = {}
lock = threading.RLock()
slots = threading.Semaphore(MAX_JOBS)

def now():
    return int(time.time())

def safe_name(name):
    name = re.sub(r'[\\/\x00-\x1f:*?"<>|]', "_", str(name or "video"))
    name = re.sub(r"\s+", " ", name).strip(" .")
    return (name or "video")[:180]

def ext_from_url(url):
    try:
        path = unquote(urlparse(url).path)
        m = re.search(r"\.([a-z0-9]{2,8})$", path, re.I)
        return m.group(1).lower() if m else ""
    except Exception:
        return ""

def media_type(url, content_type=""):
    ext = ext_from_url(url)
    if ext in MEDIA_EXTS:
        return ext, MEDIA_EXTS[ext]
    ct = (content_type or "").split(";", 1)[0].lower()
    if ct in {"application/vnd.apple.mpegurl", "application/x-mpegurl"}:
        return "m3u8", ct
    if ct == "application/dash+xml":
        return "mpd", ct
    if MEDIA_MIME_RE.match(ct):
        return (ext or "video"), ct
    return None, None

def validate_public_url(value):
    try:
        u = urlparse(str(value or "").strip())
        if u.scheme not in {"http", "https"} or not u.hostname:
            return False, "Only http:// and https:// URLs are supported."
        host = u.hostname.lower().rstrip(".")
        if host in {"localhost", "localhost.localdomain"} or host.endswith(".local"):
            return False, "Local URLs are not allowed."
        try:
            ips = socket.getaddrinfo(host, None)
        except Exception:
            return False, "Host could not be resolved."
        for item in ips:
            ip = item[4][0]
            addr = socket.inet_pton(socket.AF_INET6, ip) if ":" in ip else socket.inet_aton(ip)
            if ":" in ip:
                import ipaddress
                obj = ipaddress.ip_address(ip)
            else:
                import ipaddress
                obj = ipaddress.ip_address(ip)
            if obj.is_private or obj.is_loopback or obj.is_link_local or obj.is_reserved or obj.is_multicast:
                return False, "Private or local network URLs are not allowed."
        return True, ""
    except Exception:
        return False, "Invalid URL."

def headers_for(url, referer=None):
    h = {
        "User-Agent": "Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36 Chrome/131 Mobile Safari/537.36",
        "Accept": "*/*",
        "Accept-Encoding": "identity",
    }
    if referer:
        h["Referer"] = referer
    return h

def fetch(url, referer=None, timeout=20, max_bytes=None):
    ok, err = validate_public_url(url)
    if not ok:
        raise ValueError(err)
    req = Request(url, headers=headers_for(url, referer))
    with urlopen(req, timeout=timeout, context=ssl.create_default_context()) as resp:
        data = b""
        if max_bytes is not None:
            while len(data) < max_bytes:
                chunk = resp.read(min(1024 * 256, max_bytes - len(data)))
                if not chunk:
                    break
                data += chunk
        else:
            data = resp.read()
        return resp, data

def head_size(url, referer=None):
    ok, _ = validate_public_url(url)
    if not ok:
        return None, None
    try:
        req = Request(url, method="HEAD", headers=headers_for(url, referer))
        with urlopen(req, timeout=12) as r:
            size = r.headers.get("Content-Length")
            ct = r.headers.get("Content-Type", "")
            return (int(size) if size and size.isdigit() else None), ct
    except Exception:
        return None, None

def size_info(value):
    if not value or value <= 0:
        return {"bytes": None, "display": "Unknown"}
    units = ["B", "KB", "MB", "GB", "TB"]
    n = float(value); i = 0
    while n >= 1024 and i < len(units) - 1:
        n /= 1024; i += 1
    return {"bytes": int(value), "display": f"{n:.2f} {units[i]}"}

class MediaParser(HTMLParser):
    def __init__(self, base):
        super().__init__(convert_charrefs=True)
        self.base = base
        self.found = []
    def add(self, value, kind="media"):
        if not value:
            return
        value = html.unescape(str(value)).strip()
        if value.startswith(("data:", "blob:", "#", "javascript:")):
            return
        absolute = urljoin(self.base, value)
        if absolute.startswith(("http://", "https://")):
            self.found.append((absolute, kind))
    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        t = tag.lower()
        if t in {"video", "audio", "source", "track"}:
            for key in ("src", "data-src", "data-video", "data-url"):
                self.add(a.get(key), t)
            self.add(a.get("srcset"), t)
        elif t in {"a", "link"}:
            href = a.get("href")
            if href:
                self.add(href, t)
        elif t in {"img", "iframe", "embed", "object"}:
            for key in ("src", "data-src", "data-url", "data"):
                self.add(a.get(key), t)
        elif t == "meta":
            if a.get("property", "").lower() in {"og:video", "og:video:url", "og:video:secure_url"}:
                self.add(a.get("content"), "og:video")
        for key in ("data-src", "data-video-url", "data-file", "data-media", "data-hls", "data-m3u8"):
            self.add(a.get(key), t)
    def handle_data(self, data):
        for u in URL_RE.findall(data or ""):
            self.add(u, "text")

def classify(url, content_type="", source=""):
    ext, mime = media_type(url, content_type)
    if not ext:
        return None
    kind = "hls" if ext == "m3u8" else "dash" if ext == "mpd" else "video"
    return {"url": url, "extension": ext, "mime_type": mime or content_type or "application/octet-stream", "kind": kind, "source": source}

def scan_page(page_url):
    ok, err = validate_public_url(page_url)
    if not ok:
        raise ValueError(err)
    resp, body = fetch(page_url, max_bytes=MAX_HTML)
    final_url = resp.geturl()
    content_type = resp.headers.get("Content-Type", "")
    page_title = ""
    if "html" not in content_type.lower() and not body.lstrip().lower().startswith(b"<"):
        item = classify(final_url, content_type, "page")
        return {"title": final_url, "url": final_url, "media": [item] if item else []}
    text = body.decode("utf-8", "ignore")
    title_match = re.search(r"<title[^>]*>(.*?)</title>", text, re.I | re.S)
    if title_match:
        page_title = re.sub(r"\s+", " ", html.unescape(re.sub("<[^>]+>", "", title_match.group(1)))).strip()
    parser = MediaParser(final_url)
    parser.feed(text)
    candidates = parser.found[:]
    # Also inspect quoted media-looking URLs in scripts/JSON.
    for u in URL_RE.findall(text):
        candidates.append((u.rstrip("\\),]}"), "script"))
    seen = set(); media = []
    for u, source in candidates:
        item = classify(u, "", source)
        if not item or item["url"] in seen:
            continue
        seen.add(item["url"])
        size, ct = head_size(item["url"], final_url)
        if ct and item["extension"] in {"mp4", "webm", "mov", "avi", "mpg", "mpeg", "m4v"}:
            item["mime_type"] = ct.split(";", 1)[0]
        item["size"] = size_info(size)
        item["filesize"] = size
        media.append(item)
    # Some servers hide media behind extensionless URLs. Probe Content-Type only
    # for candidates already discovered from media attributes.
    if not media:
        for u, source in candidates[:30]:
            try:
                size, ct = head_size(u, final_url)
                item = classify(u, ct, source)
                if item and item["url"] not in seen:
                    item["size"] = size_info(size); item["filesize"] = size; media.append(item)
            except Exception:
                pass
    media.sort(key=lambda x: (0 if x["kind"] == "video" else 1, x["extension"], -(x["filesize"] or 0)))
    return {"title": page_title or final_url, "url": final_url, "media": media[:100]}

def update_job(job_id, **values):
    with lock:
        job = jobs.get(job_id)
        if job:
            job.update(values)
            job["updatedAt"] = now()

def run_ffmpeg(args):
    subprocess.run(["ffmpeg", "-y", *args], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)

def download_direct(job_id, url, output, referer=None):
    req = Request(url, headers=headers_for(url, referer))
    with urlopen(req, timeout=60, context=ssl.create_default_context()) as r:
        total = int(r.headers.get("Content-Length") or 0)
        done = 0
        with open(output, "wb") as f:
            while True:
                chunk = r.read(1024 * 1024)
                if not chunk: break
                f.write(chunk); done += len(chunk)
                update_job(job_id, downloaded=done, total=total, progress=(round(done * 100 / total, 2) if total else 0))
    return done

def download_worker(job_id, url, extension, mime, referer=None):
    with slots:
        temp = ROOT / f".tmp-{job_id}"
        temp.mkdir(parents=True, exist_ok=True)
        try:
            update_job(job_id, status="downloading", progress=0)
            parsed = urlparse(url)
            base_name = safe_name(Path(unquote(parsed.path)).stem or "downly-video")
            ext = extension.lower().lstrip(".") or "mp4"
            if ext in {"m3u8", "mpd"}:
                ext = "mp4"
                output = ROOT / f"{job_id}_{base_name}.{ext}"
                update_job(job_id, status="processing")
                run_ffmpeg(["-i", url, "-c", "copy", "-movflags", "+faststart", str(output)])
            else:
                output = ROOT / f"{job_id}_{base_name}.{ext}"
                download_direct(job_id, url, output, referer)
            total = output.stat().st_size if output.exists() else 0
            update_job(job_id, status="ready", progress=100, downloaded=total, total=total,
                       file=str(output), filename=output.name,
                       mime=mimetypes.guess_type(output.name)[0] or mime or "application/octet-stream")
        except Exception as exc:
            update_job(job_id, status="failed", error=str(exc))
        finally:
            shutil.rmtree(temp, ignore_errors=True)

def auth(handler):
    if not TOKEN:
        return False
    return handler.headers.get("Authorization", "") == f"Bearer {TOKEN}"

class Handler(BaseHTTPRequestHandler):
    server_version = "DownlyWorker-WebMedia/2.0"
    def log_message(self, fmt, *args):
        print(f"[{time.strftime('%H:%M:%S')}] " + (fmt % args))
    def cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Authorization, Content-Type, Range")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, HEAD, OPTIONS")
        self.send_header("Access-Control-Expose-Headers", "Content-Length, Content-Disposition, Content-Type, Content-Range, Accept-Ranges")
    def send_json(self, code, data):
        body = json.dumps(data, ensure_ascii=False).encode()
        self.send_response(code); self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body))); self.send_header("Cache-Control", "no-store"); self.cors(); self.end_headers(); self.wfile.write(body)
    def require_auth(self):
        if not TOKEN:
            self.send_json(503, {"ok": False, "error": "WORKER_TOKEN is not configured"}); return False
        if not auth(self):
            self.send_json(401, {"ok": False, "error": "unauthorized"}); return False
        return True
    def body(self):
        length = int(self.headers.get("Content-Length", "0"))
        if length > 1024 * 1024: raise ValueError("request too large")
        return json.loads(self.rfile.read(length) or b"{}")
    def do_OPTIONS(self):
        self.send_response(204); self.cors(); self.end_headers()
    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            self.send_json(200, {"ok": True, "service": "downly-worker", "engine": "web-media-scanner", "version": "2.0", "jobs": len(jobs), "maxJobs": MAX_JOBS, "ffmpeg": shutil.which("ffmpeg") is not None}); return
        if not self.require_auth(): return
        parts = [x for x in parsed.path.split("/") if x]
        if len(parts) == 3 and parts[:2] == ["v1", "jobs"]:
            with lock: job = jobs.get(parts[2])
            self.send_json(200 if job else 404, job or {"ok": False, "error": "job not found"}); return
        if len(parts) == 4 and parts[:2] == ["v1", "jobs"] and parts[3] == "file":
            self.serve_file(parts[2]); return
        if parsed.path == "/v1/stream":
            q = {k: v[0] for k, v in __import__("urllib.parse", fromlist=["parse_qs"]).parse_qs(parsed.query).items() if v}
            self.stream(q); return
        self.send_json(404, {"ok": False, "error": "not found"})
    def do_POST(self):
        parsed = urlparse(self.path)
        try: data = self.body()
        except Exception as exc: self.send_json(400, {"ok": False, "error": str(exc)}); return
        if parsed.path == "/v1/crawl":
            if not self.require_auth(): return
            self.crawl(data); return
        if parsed.path == "/v1/stream":
            if not self.require_auth(): return
            self.stream(data); return
        if parsed.path == "/v1/jobs":
            if not self.require_auth(): return
            self.create_job(data); return
        parts = [x for x in parsed.path.split("/") if x]
        if len(parts) == 4 and parts[:2] == ["v1", "jobs"] and parts[3] == "cancel":
            if not self.require_auth(): return
            with lock: job = jobs.get(parts[2])
            if not job: self.send_json(404, {"ok": False, "error": "job not found"}); return
            update_job(parts[2], status="cancelled"); self.send_json(200, job); return
        self.send_json(404, {"ok": False, "error": "not found"})
    def crawl(self, data):
        page = str(data.get("url") or "").strip()
        try:
            result = scan_page(page)
            self.send_json(200, {"ok": True, "success": True, **result, "mediaCount": len(result["media"])})
        except Exception as exc:
            self.send_json(422, {"ok": False, "success": False, "error": str(exc)})
    def stream(self, data):
        url = str(data.get("url") or "").strip()
        ok, err = validate_public_url(url)
        if not ok: self.send_json(400, {"ok": False, "error": err}); return
        ext = str(data.get("extension") or ext_from_url(url)).lower()
        if ext not in MEDIA_EXTS:
            self.send_json(400, {"ok": False, "error": "Unsupported media URL"}); return
        try:
            self.send_response(302); self.send_header("Location", url); self.send_header("Cache-Control", "no-store"); self.cors(); self.end_headers()
        except Exception as exc: self.send_json(502, {"ok": False, "error": str(exc)})
    def create_job(self, data):
        url = str(data.get("url") or "").strip()
        extension = str(data.get("extension") or ext_from_url(url) or "mp4").lower()
        mime = str(data.get("mime_type") or MEDIA_EXTS.get(extension, "application/octet-stream"))
        referer = str(data.get("referer") or "").strip() or None
        ok, err = validate_public_url(url)
        if not ok: self.send_json(400, {"ok": False, "error": err}); return
        if extension not in MEDIA_EXTS: self.send_json(400, {"ok": False, "error": "Unsupported media format"}); return
        job_id = uuid.uuid4().hex
        job = {"id": job_id, "url": url, "extension": extension, "mime": mime, "referer": referer,
               "status": "queued", "progress": 0, "downloaded": 0, "total": 0, "filename": None, "file": None, "error": None,
               "createdAt": now(), "updatedAt": now()}
        with lock: jobs[job_id] = job
        threading.Thread(target=download_worker, args=(job_id, url, extension, mime, referer), daemon=True).start()
        self.send_json(202, {"ok": True, "success": True, "job_id": job_id, "id": job_id,
                             "status": "queued", "progress_url": f"/v1/jobs/{job_id}", "file_url": f"/v1/jobs/{job_id}/file"})
    def serve_file(self, job_id):
        with lock: job = jobs.get(job_id)
        if not job or job.get("status") != "ready": self.send_json(404, {"ok": False, "error": "file not ready"}); return
        path = Path(job["file"])
        if not path.exists(): self.send_json(404, {"ok": False, "error": "file missing"}); return
        size = path.stat().st_size
        self.send_response(200); self.send_header("Content-Type", job.get("mime") or "application/octet-stream")
        self.send_header("Content-Length", str(size)); self.send_header("Content-Disposition", f'attachment; filename="{safe_name(job.get("filename"))}"'); self.send_header("Accept-Ranges", "bytes"); self.cors(); self.end_headers()
        with open(path, "rb") as f:
            while True:
                chunk = f.read(1024 * 1024)
                if not chunk: break
                self.wfile.write(chunk)

def cleanup_loop():
    while True:
        time.sleep(300)
        cutoff = now() - JOB_TTL
        with lock: old = [j for j in jobs.values() if j.get("updatedAt", 0) < cutoff]
        for job in old:
            try:
                if job.get("file"): Path(job["file"]).unlink(missing_ok=True)
            except Exception: pass
            with lock: jobs.pop(job["id"], None)

if __name__ == "__main__":
    threading.Thread(target=cleanup_loop, daemon=True).start()
    ThreadingHTTPServer((HOST, PORT), Handler).serve_forever()
