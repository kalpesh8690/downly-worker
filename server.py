#!/usr/bin/env python3
"""
Downly Worker - persistent yt-dlp worker for Termux/Android.

API:
  GET  /health
  POST /v1/jobs
  GET  /v1/jobs/:id
  POST /v1/jobs/:id/pause
  POST /v1/jobs/:id/resume
  POST /v1/jobs/:id/cancel
  GET/HEAD /v1/jobs/:id/file

Environment:
  WORKER_TOKEN       required
  HOST               default 127.0.0.1
  PORT               default 8080
  DOWNLOAD_DIR       default ~/downly-worker/downloads
  MAX_JOBS           default 2
  JOB_TTL_SECONDS    default 86400
"""
import json
import mimetypes
import os
import re
import shutil
import signal
import subprocess
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

TOKEN = os.environ.get("WORKER_TOKEN", "")
HOST = os.environ.get("HOST", "127.0.0.1")
PORT = int(os.environ.get("PORT", "8080"))
ROOT = Path(os.environ.get("DOWNLOAD_DIR", str(Path.home() / "downly-worker" / "downloads"))).expanduser()
MAX_JOBS = max(1, int(os.environ.get("MAX_JOBS", "2")))
JOB_TTL = max(300, int(os.environ.get("JOB_TTL_SECONDS", "86400")))
ROOT.mkdir(parents=True, exist_ok=True)

jobs = {}
lock = threading.RLock()
slots = threading.Semaphore(MAX_JOBS)

VIDEO_EXTS = {".mp4", ".mkv", ".webm", ".mov", ".avi", ".m4v", ".flv"}
AUDIO_EXTS = {".mp3", ".m4a", ".opus", ".aac", ".wav", ".flac", ".ogg"}

def now():
    return int(time.time())

def job_dir(job_id):
    return ROOT / job_id

def meta_path(job_id):
    return job_dir(job_id) / "meta.json"

def safe_name(name):
    name = re.sub(r"[\\/\x00-\x1f]", "_", name or "download")
    return name[:180] or "download"

def write_meta(meta):
    p = meta_path(meta["id"])
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, p)

def read_meta(job_id):
    p = meta_path(job_id)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None

def load_jobs():
    for d in ROOT.iterdir():
        if not d.is_dir():
            continue
        m = read_meta(d.name)
        if not m:
            continue
        if m.get("status") in {"running", "paused", "queued"}:
            m["status"] = "interrupted"
            m["error"] = "Worker restarted; resume requires a new worker process."
            m["updatedAt"] = now()
            write_meta(m)
        jobs[d.name] = m

def enrich(meta):
    out = dict(meta)
    total = int(out.get("totalBytes") or 0)
    done = int(out.get("bytes") or 0)
    if total:
        out["progress"] = round(done * 100 / total, 2)
    else:
        out["progress"] = None
    if out.get("startedAt") and done:
        elapsed = max(0.001, time.time() - out["startedAt"])
        out["speedBps"] = int(done / elapsed)
        if total and out["speedBps"]:
            out["etaSeconds"] = max(0, int((total - done) / out["speedBps"]))
        else:
            out["etaSeconds"] = None
    else:
        out["speedBps"] = int(out.get("speedBps") or 0)
        out["etaSeconds"] = None
    return out

def find_output(job_id):
    d = job_dir(job_id)
    candidates = [p for p in d.iterdir() if p.is_file() and p.name not in {"meta.json"} and not p.name.endswith(".part") and not p.name.endswith(".tmp")]
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)

def parse_progress(line):
    # yt-dlp --newline progress lines contain: [download]  12.3% of 20.00MiB at 1.2MiB/s ETA 00:15
    m = re.search(r"\[download\]\s+(\d+(?:\.\d+)?)%", line)
    pct = float(m.group(1)) if m else None
    sm = re.search(r"at\s+([0-9.]+)\s*([KMG]?i?B/s)", line, re.I)
    speed = None
    if sm:
        n = float(sm.group(1))
        unit = sm.group(2).lower()
        mult = {"b/s":1,"kb/s":1000,"kib/s":1024,"mb/s":1000**2,"mib/s":1024**2,"gb/s":1000**3,"gib/s":1024**3}.get(unit, 1)
        speed = int(n * mult)
    return pct, speed

def run_job(job_id):
    with lock:
        m = jobs.get(job_id)
        if m and m.get("status") not in {"cancelled", "ready"}:
            m["status"] = "queued"
            m["updatedAt"] = now()
            write_meta(m)
    threading.Thread(target=execute_job, args=(job_id,), daemon=True).start()

def execute_job(job_id):
    slots.acquire()
    try:
        with lock:
            m = jobs.get(job_id)
            if not m:
                return
            m["status"] = "running"
            m["startedAt"] = m.get("startedAt") or time.time()
            m["updatedAt"] = now()
            write_meta(m)

        d = job_dir(job_id)
        out_tpl = str(d / "%(title).180B.%(ext)s")
        cmd = [
            "yt-dlp", "--no-playlist", "--newline",
            "--continue", "--no-overwrites",
            "--retries", "10", "--fragment-retries", "10",
            "--retry-sleep", "http:linear=1::2",
            "--socket-timeout", "30",
            "--restrict-filenames",
            "--print", "after_move:filepath",
            "-o", out_tpl,
        ]
        if m.get("audio_only"):
            cmd += ["-x", "--audio-format", "mp3", "--audio-quality", "192K"]
        else:
            cmd += ["-f", "bv*+ba/b", "--merge-output-format", "mp4"]
        cmd += [m["url"]]

        proc = subprocess.Popen(
            cmd, cwd=str(d), stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1, start_new_session=True
        )
        with lock:
            m["pid"] = proc.pid
            m["updatedAt"] = now()
            write_meta(m)

        final_path = None
        for line in proc.stdout:
            line = line.rstrip()
            pct, speed = parse_progress(line)
            with lock:
                m = jobs.get(job_id)
                if not m:
                    break
                if pct is not None:
                    m["progress"] = pct
                if speed:
                    m["speedBps"] = speed
                # Inspect the growing part/final file for byte progress.
                candidates = list(d.glob("*"))
                files = [p for p in candidates if p.is_file() and p.name != "meta.json"]
                if files:
                    largest = max(files, key=lambda p: p.stat().st_size)
                    try:
                        m["bytes"] = largest.stat().st_size
                        m["filename"] = largest.name
                    except OSError:
                        pass
                m["updatedAt"] = now()
                write_meta(m)
            if line.startswith("after_move:"):
                final_path = Path(line.split(":", 1)[1].strip())

        rc = proc.wait()
        with lock:
            m = jobs.get(job_id)
            if not m:
                return
            if m.get("status") == "cancelled":
                return
            if rc != 0:
                m["status"] = "failed"
                m["error"] = f"yt-dlp exited with code {rc}"
                m["updatedAt"] = now()
                write_meta(m)
                return

            final_path = final_path if final_path and final_path.exists() else find_output(job_id)
            if not final_path:
                m["status"] = "failed"
                m["error"] = "Download completed but output file was not found"
            else:
                size = final_path.stat().st_size
                ext = final_path.suffix.lower().lstrip(".")
                m.update({
                    "status": "ready",
                    "filename": safe_name(final_path.name),
                    "path": str(final_path.name),
                    "bytes": size,
                    "totalBytes": size,
                    "sizeBytes": size,
                    "extension": ext,
                    "mime": mimetypes.guess_type(final_path.name)[0] or "application/octet-stream",
                    "progress": 100,
                    "completedAt": now(),
                    "updatedAt": now(),
                    "pid": None,
                })
            write_meta(m)
    except Exception as e:
        with lock:
            m = jobs.get(job_id)
            if m:
                m["status"] = "failed"
                m["error"] = str(e)
                m["updatedAt"] = now()
                write_meta(m)
    finally:
        slots.release()

def auth(handler):
    if not TOKEN:
        return False
    got = handler.headers.get("Authorization", "")
    return got == f"Bearer {TOKEN}"

def json_body(handler):
    n = int(handler.headers.get("Content-Length", "0"))
    if n > 1024 * 1024:
        raise ValueError("request too large")
    raw = handler.rfile.read(n)
    return json.loads(raw or b"{}")

class Handler(BaseHTTPRequestHandler):
    server_version = "DownlyWorker/2.0"

    def log_message(self, fmt, *args):
        print("[%s] %s" % (time.strftime("%H:%M:%S"), fmt % args))

    def send_json(self, code, data):
        body = json.dumps(data, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def require_auth(self):
        if not auth(self):
            self.send_json(401, {"error": "unauthorized"})
            return False
        return True

    def do_GET(self):
        if self.path == "/health":
            self.send_json(200, {"ok": True, "service": "downly-worker", "version": "2.0.0", "jobs": len(jobs), "maxJobs": MAX_JOBS})
            return
        if not self.require_auth():
            return
        parts = [x for x in urlparse(self.path).path.split("/") if x]
        if len(parts) == 2 and parts[0] == "v1" and parts[1] == "jobs":
            with lock:
                data = [enrich(v) for v in jobs.values()]
            self.send_json(200, {"jobs": data})
            return
        if len(parts) >= 3 and parts[0] == "v1" and parts[1] == "jobs":
            job_id = parts[2]
            with lock:
                m = jobs.get(job_id)
            if not m:
                self.send_json(404, {"error": "job not found"})
                return
            if len(parts) == 4 and parts[3] == "file":
                self.serve_file(job_id, head_only=False)
                return
            self.send_json(200, enrich(m))
            return
        self.send_json(404, {"error": "not found"})

    def do_HEAD(self):
        if not self.require_auth():
            return
        parts = [x for x in urlparse(self.path).path.split("/") if x]
        if len(parts) == 4 and parts[0] == "v1" and parts[1] == "jobs" and parts[3] == "file":
            self.serve_file(parts[2], head_only=True)
            return
        self.send_response(404); self.end_headers()

    def do_POST(self):
        if not self.require_auth():
            return
        parts = [x for x in urlparse(self.path).path.split("/") if x]
        try:
            body = json_body(self)
        except Exception as e:
            self.send_json(400, {"error": str(e)})
            return

        if parts == ["v1", "jobs"]:
            url = str(body.get("url", "")).strip()
            if not re.match(r"^https?://", url, re.I):
                self.send_json(400, {"error": "url must be http/https"})
                return
            job_id = uuid.uuid4().hex
            d = job_dir(job_id); d.mkdir(parents=True, exist_ok=False)
            m = {
                "id": job_id, "url": url, "audio_only": bool(body.get("audio_only", False)),
                "status": "queued", "progress": 0, "bytes": 0, "totalBytes": None,
                "speedBps": 0, "etaSeconds": None, "createdAt": now(), "updatedAt": now(),
                "filename": None, "extension": None, "mime": None, "pid": None,
            }
            with lock:
                jobs[job_id] = m
                write_meta(m)
            run_job(job_id)
            self.send_json(202, enrich(m))
            return

        if len(parts) == 4 and parts[0] == "v1" and parts[1] == "jobs":
            job_id, action = parts[2], parts[3]
            with lock:
                m = jobs.get(job_id)
            if not m:
                self.send_json(404, {"error": "job not found"}); return
            if action == "pause":
                if m.get("pid") and m.get("status") == "running":
                    try:
                        os.killpg(m["pid"], signal.SIGSTOP)
                        with lock:
                            m["status"] = "paused"; m["updatedAt"] = now(); write_meta(m)
                        self.send_json(200, enrich(m)); return
                    except Exception as e:
                        self.send_json(409, {"error": f"pause failed: {e}"}); return
                self.send_json(409, {"error": "job is not running"}); return
            if action == "resume":
                if m.get("pid") and m.get("status") == "paused":
                    try:
                        os.killpg(m["pid"], signal.SIGCONT)
                        with lock:
                            m["status"] = "running"; m["updatedAt"] = now(); write_meta(m)
                        self.send_json(200, enrich(m)); return
                    except Exception as e:
                        self.send_json(409, {"error": f"resume failed: {e}"}); return
                if m.get("status") == "interrupted":
                    with lock:
                        m["status"] = "queued"
                        m["error"] = None
                        m["updatedAt"] = now()
                        write_meta(m)
                    run_job(job_id)
                    self.send_json(202, enrich(m)); return
                self.send_json(409, {"error": "job is not paused or interrupted"}); return
            if action == "cancel":
                pid = m.get("pid")
                if pid:
                    try: os.killpg(pid, signal.SIGTERM)
                    except ProcessLookupError: pass
                    except Exception: pass
                with lock:
                    m["status"] = "cancelled"; m["updatedAt"] = now(); m["pid"] = None
                    write_meta(m)
                self.send_json(200, enrich(m)); return
        self.send_json(404, {"error": "not found"})

    def serve_file(self, job_id, head_only=False):
        with lock:
            m = jobs.get(job_id)
        if not m:
            self.send_json(404, {"error": "job not found"}); return
        p = find_output(job_id)
        if not p or not p.exists():
            self.send_json(404, {"error": "file not ready"}); return
        size = p.stat().st_size
        mime = m.get("mime") or mimetypes.guess_type(p.name)[0] or "application/octet-stream"
        name = safe_name(m.get("filename") or p.name)
        range_header = self.headers.get("Range")
        start, end = 0, size - 1
        status = 200
        if range_header:
            mm = re.match(r"bytes=(\d*)-(\d*)$", range_header.strip())
            if not mm:
                self.send_response(416); self.send_header("Content-Range", f"bytes */{size}"); self.end_headers(); return
            a, b = mm.groups()
            if a == "":
                length = int(b)
                start = max(0, size - length); end = size - 1
            else:
                start = int(a); end = int(b) if b else size - 1
            if start >= size or start > end:
                self.send_response(416); self.send_header("Content-Range", f"bytes */{size}"); self.end_headers(); return
            end = min(end, size - 1); status = 206
        length = end - start + 1
        self.send_response(status)
        self.send_header("Content-Type", mime)
        self.send_header("Content-Disposition", f'attachment; filename="{name}"')
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Length", str(length))
        self.send_header("X-Downly-Extension", m.get("extension") or p.suffix.lstrip("."))
        self.send_header("X-Downly-Size", str(size))
        if status == 206:
            self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
        self.end_headers()
        if head_only:
            return
        with p.open("rb") as f:
            f.seek(start)
            remaining = length
            while remaining:
                chunk = f.read(min(1024 * 1024, remaining))
                if not chunk: break
                self.wfile.write(chunk)
                remaining -= len(chunk)

def cleanup_loop():
    while True:
        time.sleep(600)
        cutoff = now() - JOB_TTL
        with lock:
            for job_id, m in list(jobs.items()):
                if m.get("status") in {"ready", "failed", "cancelled", "interrupted"} and int(m.get("updatedAt", 0)) < cutoff:
                    shutil.rmtree(job_dir(job_id), ignore_errors=True)
                    jobs.pop(job_id, None)

load_jobs()
threading.Thread(target=cleanup_loop, daemon=True).start()

if __name__ == "__main__":
    if not TOKEN:
        raise SystemExit("WORKER_TOKEN is required")
    print(f"Downly Worker 2.0 listening on http://{HOST}:{PORT}")
    print(f"Download store: {ROOT}")
    print(f"Max concurrent jobs: {MAX_JOBS}")
    ThreadingHTTPServer((HOST, PORT), Handler).serve_forever()
