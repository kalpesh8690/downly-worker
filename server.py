#!/usr/bin/env python3
"""
Downly Worker - pytubefix engine.

No yt-dlp is used by this worker.

API:
  GET  /health
  POST /v1/info
  POST /v1/inspect
  POST /v1/stream
  POST /v1/jobs
  GET  /v1/jobs
  GET  /v1/jobs/:id
  GET  /v1/jobs/:id/file
  POST /v1/jobs/:id/cancel
"""
import json
import mimetypes
import os
import re
import shutil
import subprocess
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs, quote
from urllib.request import Request, urlopen

from pytubefix import YouTube

TOKEN = os.environ.get("WORKER_TOKEN", "")
HOST = os.environ.get("HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", "8080"))
ROOT = Path(os.environ.get("DOWNLOAD_DIR", str(Path.home() / "downly-worker" / "downloads"))).expanduser()
MAX_JOBS = max(1, int(os.environ.get("MAX_JOBS", "2")))
JOB_TTL = max(300, int(os.environ.get("JOB_TTL_SECONDS", "86400")))
ROOT.mkdir(parents=True, exist_ok=True)

jobs = {}
lock = threading.RLock()
slots = threading.Semaphore(MAX_JOBS)

YOUTUBE_HOSTS = {
    "youtube.com", "www.youtube.com", "m.youtube.com",
    "youtu.be", "www.youtu.be",
    "youtube-nocookie.com", "www.youtube-nocookie.com",
}

def now():
    return int(time.time())

def valid_youtube_url(value):
    try:
        p = urlparse(str(value or "").strip())
        host = (p.hostname or "").lower().rstrip(".")
        return (
            p.scheme in {"http", "https"}
            and any(host == h or host.endswith("." + h) for h in YOUTUBE_HOSTS)
        )
    except Exception:
        return False

def safe_name(name):
    name = re.sub(r'[\\/\x00-\x1f:*?"<>|]', "_", str(name or "video"))
    name = re.sub(r"\s+", " ", name).strip(" .")
    return (name or "video")[:180]

def format_size(value):
    try:
        n = int(value)
    except Exception:
        return {"bytes": None, "display": "Unknown"}
    if n <= 0:
        return {"bytes": None, "display": "Unknown"}
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(n)
    unit = 0
    while size >= 1024 and unit < len(units) - 1:
        size /= 1024
        unit += 1
    return {"bytes": n, "display": f"{size:.2f} {units[unit]}"}

def stream_info(stream):
    vcodec = getattr(stream, "video_codec", None)
    acodec = getattr(stream, "audio_codec", None)
    video = bool(vcodec)
    audio = bool(acodec)
    kind = "video_audio" if video and audio else "video" if video else "audio" if audio else "unknown"
    size = getattr(stream, "filesize", None) or getattr(stream, "filesize_approx", None)
    return {
        "id": str(getattr(stream, "itag", "")),
        "itag": getattr(stream, "itag", None),
        "type": kind,
        "mime_type": getattr(stream, "mime_type", None),
        "extension": getattr(stream, "subtype", None),
        "resolution": getattr(stream, "resolution", None),
        "width": getattr(stream, "width", None),
        "height": getattr(stream, "height", None),
        "fps": getattr(stream, "fps", None),
        "video_codec": vcodec,
        "audio_codec": acodec,
        "progressive": bool(getattr(stream, "is_progressive", False)),
        "adaptive": bool(getattr(stream, "is_adaptive", False)),
        "abr": getattr(stream, "abr", None),
        "bitrate": getattr(stream, "bitrate", None),
        "size": format_size(size),
        "filesize": size,
        "hasAudio": audio,
        "hasVideo": video,
        "includes_audio": audio,
        "includes_video": video,
    }

def get_stream(url, itag):
    if not valid_youtube_url(url):
        raise ValueError("Invalid YouTube URL")
    yt = YouTube(url)
    stream = yt.streams.get_by_itag(int(itag))
    if not stream:
        raise ValueError("Format not found")
    return yt, stream

def update_job(job_id, **values):
    with lock:
        job = jobs.get(job_id)
        if job:
            job.update(values)
            job["updatedAt"] = now()

def progress_callback(job_id):
    def callback(stream, chunk, bytes_remaining):
        total = getattr(stream, "filesize", None) or getattr(stream, "filesize_approx", None)
        if total:
            downloaded = max(0, int(total) - int(bytes_remaining))
            update_job(
                job_id,
                progress=round(downloaded * 100 / total, 2),
                downloaded=downloaded,
                total=total,
            )
    return callback

def run_ffmpeg(args):
    subprocess.run(
        ["ffmpeg", "-y", *args],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )

def choose_audio(yt):
    return (
        yt.streams.filter(only_audio=True)
        .order_by("abr").desc()
        .first()
    )

def download_worker(job_id, url, itag, mode, output_format):
    with slots:
        temp = ROOT / f".tmp-{job_id}"
        temp.mkdir(parents=True, exist_ok=True)
        try:
            update_job(job_id, status="starting", progress=0)
            yt = YouTube(url, on_progress_callback=progress_callback(job_id))
            update_job(job_id, title=yt.title, thumbnail=yt.thumbnail_url, status="downloading")

            selected = yt.streams.get_by_itag(int(itag))
            if not selected:
                raise RuntimeError("Format not found")

            title = safe_name(yt.title)

            if mode == "audio":
                if output_format not in {"mp3", "m4a", "wav"}:
                    raise RuntimeError("Unsupported audio format")
                source = selected.download(output_path=str(temp))
                update_job(job_id, status="converting", progress=90)
                output = ROOT / f"{job_id}_{title}.{output_format}"
                if output_format == "mp3":
                    run_ffmpeg(["-i", source, "-vn", "-c:a", "libmp3lame", "-b:a", "192k", str(output)])
                elif output_format == "m4a":
                    run_ffmpeg(["-i", source, "-vn", "-c:a", "aac", "-b:a", "192k", str(output)])
                else:
                    run_ffmpeg(["-i", source, "-vn", str(output)])

            elif getattr(selected, "is_progressive", False):
                source = selected.download(output_path=str(temp))
                ext = getattr(selected, "subtype", None) or "mp4"
                output = ROOT / f"{job_id}_{title}.{ext}"
                shutil.move(source, output)

            else:
                video = selected.download(output_path=str(temp))
                update_job(job_id, status="downloading_audio", progress=50)
                audio = choose_audio(yt)
                if not audio:
                    raise RuntimeError("Compatible audio stream not found")
                audio_file = audio.download(output_path=str(temp))
                update_job(job_id, status="merging", progress=90)
                output = ROOT / f"{job_id}_{title}.mp4"
                run_ffmpeg([
                    "-i", video, "-i", audio_file,
                    "-map", "0:v:0", "-map", "1:a:0",
                    "-c:v", "copy", "-c:a", "aac",
                    "-movflags", "+faststart", str(output),
                ])

            total = output.stat().st_size if output.exists() else 0
            update_job(
                job_id,
                status="ready",
                progress=100,
                downloaded=total,
                total=total,
                file=str(output),
                filename=output.name,
                mime=mimetypes.guess_type(output.name)[0] or "application/octet-stream",
            )
        except Exception as exc:
            update_job(job_id, status="failed", error=str(exc))
        finally:
            shutil.rmtree(temp, ignore_errors=True)

def auth(handler):
    if not TOKEN:
        return False
    return handler.headers.get("Authorization", "") == f"Bearer {TOKEN}"

class Handler(BaseHTTPRequestHandler):
    server_version = "DownlyWorker-Pytubefix/1.0"

    def log_message(self, fmt, *args):
        print(f"[{time.strftime('%H:%M:%S')}] " + (fmt % args))

    def cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Authorization, Content-Type, Range")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, HEAD, OPTIONS")

    def send_json(self, code, data):
        body = json.dumps(data, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.cors()
        self.end_headers()
        self.wfile.write(body)

    def require_auth(self):
        if not auth(self):
            self.send_json(401, {"ok": False, "error": "unauthorized"})
            return False
        return True

    def body(self):
        length = int(self.headers.get("Content-Length", "0"))
        if length > 1024 * 1024:
            raise ValueError("request too large")
        return json.loads(self.rfile.read(length) or b"{}")

    def do_OPTIONS(self):
        self.send_response(204)
        self.cors()
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            self.send_json(200, {
                "ok": True,
                "service": "downly-worker",
                "engine": "pytubefix",
                "version": "1.0.0",
                "jobs": len(jobs),
                "maxJobs": MAX_JOBS,
                "ffmpeg": shutil.which("ffmpeg") is not None,
            })
            return

        if not self.require_auth():
            return

        parts = [x for x in parsed.path.split("/") if x]
        if parsed.path == "/v1/jobs":
            with lock:
                self.send_json(200, {"jobs": list(jobs.values())})
            return

        if len(parts) == 3 and parts[:2] == ["v1", "jobs"]:
            with lock:
                job = jobs.get(parts[2])
            if not job:
                self.send_json(404, {"error": "job not found"})
            else:
                self.send_json(200, job)
            return

        if len(parts) == 4 and parts[:2] == ["v1", "jobs"] and parts[3] == "file":
            self.serve_file(parts[2])
            return

        if parsed.path == "/v1/stream":
            self.stream_from_query(parse_qs(parsed.query))
            return

        self.send_json(404, {"error": "not found"})

    def do_HEAD(self):
        if not self.require_auth():
            return
        parts = [x for x in urlparse(self.path).path.split("/") if x]
        if len(parts) == 4 and parts[:2] == ["v1", "jobs"] and parts[3] == "file":
            self.serve_file(parts[2], head_only=True)
            return
        self.send_response(404)
        self.cors()
        self.end_headers()

    def do_POST(self):
        if not self.require_auth():
            return
        parsed = urlparse(self.path)
        try:
            data = self.body()
        except Exception as exc:
            self.send_json(400, {"ok": False, "error": str(exc)})
            return

        if parsed.path in {"/v1/info", "/v1/inspect"}:
            self.info(data)
            return

        if parsed.path == "/v1/stream":
            self.stream_body(data)
            return

        if parsed.path == "/v1/jobs":
            self.create_job(data)
            return

        parts = [x for x in parsed.path.split("/") if x]
        if len(parts) == 4 and parts[:2] == ["v1", "jobs"]:
            job_id, action = parts[2], parts[3]
            if action == "cancel":
                with lock:
                    job = jobs.get(job_id)
                    if not job:
                        self.send_json(404, {"error": "job not found"})
                        return
                    job["status"] = "cancelled"
                    job["updatedAt"] = now()
                self.send_json(200, job)
                return

        self.send_json(404, {"error": "not found"})

    def info(self, data):
        url = str(data.get("url") or "").strip()
        if not valid_youtube_url(url):
            self.send_json(400, {"ok": False, "error": "Invalid YouTube URL"})
            return
        try:
            yt = YouTube(url)
            formats = []
            for stream in yt.streams:
                try:
                    formats.append(stream_info(stream))
                except Exception:
                    pass

            formats.sort(
                key=lambda x: (
                    0 if x["hasVideo"] else 1,
                    -(int(x.get("height") or 0)),
                    -(float(x.get("fps") or 0)),
                    -(int(x.get("filesize") or 0)),
                )
            )

            self.send_json(200, {
                "ok": True,
                "success": True,
                "url": url,
                "id": yt.video_id,
                "title": yt.title,
                "author": yt.author,
                "thumbnail": yt.thumbnail_url,
                "duration": yt.length,
                "durationString": str(yt.length),
                "viewCount": getattr(yt, "views", None),
                "webpageUrl": url,
                "formatCount": len(formats),
                "formats": formats[:120],
            })
        except Exception as exc:
            self.send_json(422, {"ok": False, "success": False, "error": str(exc)})

    def stream_body(self, data):
        url = str(data.get("url") or "").strip()
        direct = str(data.get("direct_url") or "").strip()
        itag = data.get("itag") or data.get("format")
        if direct and re.match(r"^https?://", direct):
            self.proxy_direct(direct, bool(data.get("preview")))
            return
        if not url or not itag:
            self.send_json(400, {"ok": False, "error": "url and itag are required"})
            return
        try:
            _, stream = get_stream(url, itag)
            # Redirecting lets ExoPlayer/browser stream directly from YouTube.
            self.send_response(302)
            self.send_header("Location", stream.url)
            self.send_header("Cache-Control", "no-store")
            self.cors()
            self.end_headers()
        except Exception as exc:
            self.send_json(422, {"ok": False, "error": str(exc)})

    def stream_from_query(self, query):
        data = {k: v[0] for k, v in query.items() if v}
        self.stream_body(data)

    def proxy_direct(self, url, preview=False):
        try:
            req = Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "*/*"})
            with urlopen(req, timeout=30) as response:
                self.send_response(200)
                self.send_header("Content-Type", response.headers.get("Content-Type", "application/octet-stream"))
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Disposition", "inline" if preview else "attachment")
                self.cors()
                self.end_headers()
                while True:
                    chunk = response.read(1024 * 1024)
                    if not chunk:
                        break
                    self.wfile.write(chunk)
        except Exception as exc:
            self.send_json(502, {"ok": False, "error": str(exc)})

    def create_job(self, data):
        url = str(data.get("url") or "").strip()
        itag = data.get("itag") or data.get("format")
        mode = str(data.get("mode") or "video").lower()
        output_format = str(data.get("output_format") or "mp4").lower()
        if not valid_youtube_url(url) or not itag:
            self.send_json(400, {"ok": False, "error": "url and itag are required"})
            return
        if mode not in {"video", "audio"}:
            self.send_json(400, {"ok": False, "error": "Invalid mode"})
            return
        if mode == "audio" and output_format not in {"mp3", "m4a", "wav"}:
            self.send_json(400, {"ok": False, "error": "Invalid audio format"})
            return

        job_id = uuid.uuid4().hex
        job = {
            "id": job_id, "url": url, "format": str(itag), "mode": mode,
            "status": "queued", "progress": 0, "downloaded": 0, "total": 0,
            "speedBps": 0, "etaSeconds": None, "title": None,
            "filename": None, "file": None, "error": None,
            "createdAt": now(), "updatedAt": now(),
        }
        with lock:
            jobs[job_id] = job

        threading.Thread(
            target=download_worker,
            args=(job_id, url, itag, mode, output_format),
            daemon=True,
        ).start()

        self.send_json(202, {
            "ok": True,
            "success": True,
            "job_id": job_id,
            "id": job_id,
            "status": "queued",
            "progress_url": f"/v1/jobs/{job_id}",
            "file_url": f"/v1/jobs/{job_id}/file",
        })

    def serve_file(self, job_id, head_only=False):
        with lock:
            job = jobs.get(job_id)
        if not job:
            self.send_json(404, {"error": "job not found"})
            return
        if job.get("status") != "ready":
            self.send_json(409, {"error": "File is not ready", "status": job.get("status")})
            return
        path = Path(job.get("file") or "")
        if not path.exists():
            self.send_json(404, {"error": "file no longer exists"})
            return

        size = path.stat().st_size
        mime = job.get("mime") or mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        start, end = 0, size - 1
        status = 200
        rng = self.headers.get("Range")
        if rng:
            match = re.match(r"bytes=(\d*)-(\d*)$", rng.strip())
            if not match:
                self.send_response(416); self.cors(); self.end_headers(); return
            a, b = match.groups()
            if a:
                start = int(a)
                end = int(b) if b else size - 1
            else:
                length = int(b)
                start = max(0, size - length)
            end = min(end, size - 1)
            if start >= size or start > end:
                self.send_response(416); self.cors(); self.end_headers(); return
            status = 206

        length = end - start + 1
        self.send_response(status)
        self.send_header("Content-Type", mime)
        self.send_header("Content-Length", str(length))
        self.send_header("Content-Disposition", f'attachment; filename="{safe_name(job.get("filename") or path.name)}"')
        self.send_header("Accept-Ranges", "bytes")
        if status == 206:
            self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
        self.cors()
        self.end_headers()
        if head_only:
            return

        with path.open("rb") as f:
            f.seek(start)
            remaining = length
            while remaining:
                chunk = f.read(min(1024 * 1024, remaining))
                if not chunk:
                    break
                self.wfile.write(chunk)
                remaining -= len(chunk)

def cleanup_loop():
    while True:
        time.sleep(600)
        cutoff = now() - JOB_TTL
        with lock:
            for job_id, job in list(jobs.items()):
                if job.get("status") in {"ready", "failed", "cancelled"} and int(job.get("updatedAt", 0)) < cutoff:
                    shutil.rmtree(ROOT / f".tmp-{job_id}", ignore_errors=True)
                    path = job.get("file")
                    if path:
                        try:
                            Path(path).unlink(missing_ok=True)
                        except Exception:
                            pass
                    jobs.pop(job_id, None)

threading.Thread(target=cleanup_loop, daemon=True).start()

if __name__ == "__main__":
    if not TOKEN:
        raise SystemExit("WORKER_TOKEN is required")
    print(f"Downly Worker pytubefix listening on http://{HOST}:{PORT}")
    ThreadingHTTPServer((HOST, PORT), Handler).serve_forever()
