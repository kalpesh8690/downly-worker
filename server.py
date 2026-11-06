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
import logging
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("downly-worker")

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
            m["error"] = "Worker restarted; job can be resumed."
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

def parse_size(value):
    if not value:
        return None
    m = re.match(r"([0-9.]+)\s*([KMGTP]?i?B)", value.strip(), re.I)
    if not m:
        return None
    n = float(m.group(1))
    unit = m.group(2).lower()
    mult = {
        "b": 1, "kb": 1000, "kib": 1024, "mb": 1000**2, "mib": 1024**2,
        "gb": 1000**3, "gib": 1024**3, "tb": 1000**4, "tib": 1024**4,
        "pb": 1000**5, "pib": 1024**5,
    }.get(unit, 1)
    return int(n * mult)

def parse_eta(value):
    if not value or value in {"Unknown", "N/A", "--:--"}:
        return None
    parts = value.strip().split(":")
    try:
        if len(parts) == 3:
            h, m, s = map(int, parts)
            return h * 3600 + m * 60 + s
        if len(parts) == 2:
            m, s = map(int, parts)
            return m * 60 + s
        return int(float(value))
    except Exception:
        return None

def parse_progress(line):
    # Prefer the machine-readable yt-dlp progress template.
    if line.startswith("download:"):
        parts = line.split("|", 5)
        if len(parts) == 6:
            _, pct_raw, bytes_raw, total_raw, speed_raw, eta_raw = parts
            try:
                pct = float(str(pct_raw).replace("%", "").strip()) if pct_raw not in {"NA", "N/A", ""} else None
            except Exception:
                pct = None
            try:
                downloaded = int(float(bytes_raw)) if bytes_raw not in {"NA", "N/A", ""} else None
            except Exception:
                downloaded = None
            try:
                total = int(float(total_raw)) if total_raw not in {"NA", "N/A", ""} else None
            except Exception:
                total = None
            try:
                speed = int(float(speed_raw)) if speed_raw not in {"NA", "N/A", ""} else None
            except Exception:
                speed = None
            eta = parse_eta(eta_raw)
            return pct, total, speed, eta, downloaded

    m = re.search(r"\[download\]\s+(\d+(?:\.\d+)?)%", line)
    pct = float(m.group(1)) if m else None
    total = None
    tm = re.search(r"\bof\s+(?:~\s*)?([0-9.]+\s*[KMGTP]?i?B)", line, re.I)
    if tm:
        total = parse_size(tm.group(1))
    sm = re.search(r"\bat\s+([0-9.]+)\s*([KMG]?i?B/s)", line, re.I)
    speed = None
    if sm:
        n = float(sm.group(1))
        unit = sm.group(2).lower()
        mult = {"b/s":1,"kb/s":1000,"kib/s":1024,"mb/s":1000**2,"mib/s":1024**2,"gb/s":1000**3,"gib/s":1024**3}.get(unit, 1)
        speed = int(n * mult)
    eta = None
    em = re.search(r"\bETA\s+([0-9:]+)", line, re.I)
    if em:
        eta = parse_eta(em.group(1))
    return pct, total, speed, eta, None

def inspect_url(url):
    cmd = [
        "yt-dlp", "--dump-single-json", "--skip-download", "--no-playlist",
        "--no-warnings", "--no-cache-dir", "--socket-timeout", "20", url
    ]
    log.info("inspect url=%s", url)
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=90)
    output = proc.stdout or ""
    if proc.returncode != 0:
        lines = [x.strip() for x in output.splitlines() if x.strip()]
        raise RuntimeError(lines[-1] if lines else "Unable to inspect this media URL")
    raw = json.loads(output)
    formats = []
    seen = set()
    for f in raw.get("formats") or []:
        fid = str(f.get("format_id") or "")
        if not fid or fid in seen:
            continue
        seen.add(fid)
        height = f.get("height")
        width = f.get("width")
        vcodec = str(f.get("vcodec") or "none")
        acodec = str(f.get("acodec") or "none")
        has_video = vcodec != "none"
        has_audio = acodec != "none"
        ext = str(f.get("ext") or "")
        if not has_video and not has_audio:
            continue
        if has_video:
            label = (str(height) + "p") if height else (str(width) + "w" if width else "Video")
            if f.get("fps"):
                label += " " + str(f.get("fps")) + "fps"
        else:
            label = "Audio"
        if ext:
            label += " · " + ext.upper()
        if has_video and not has_audio:
            selector = fid + "+bestaudio/" + fid
        else:
            selector = fid
        formats.append({
            "id": fid,
            "selector": selector,
            "label": label,
            "ext": ext,
            "height": height,
            "width": width,
            "fps": f.get("fps"),
            "filesize": f.get("filesize") or f.get("filesize_approx"),
            "tbr": f.get("tbr"),
            "vcodec": vcodec,
            "acodec": acodec,
            "hasVideo": has_video,
            "hasAudio": has_audio,
            "protocol": f.get("protocol"),
        })
    formats.sort(key=lambda x: (
        0 if x["hasVideo"] else 1,
        -(int(x["height"] or 0)),
        -(float(x["tbr"] or 0)),
        x["ext"] or "zzz"
    ))
    return {
        "ok": True,
        "url": url,
        "webpageUrl": raw.get("webpage_url") or url,
        "id": raw.get("id"),
        "title": raw.get("title") or "Untitled media",
        "author": raw.get("uploader") or raw.get("channel") or raw.get("creator"),
        "thumbnail": raw.get("thumbnail"),
        "duration": raw.get("duration"),
        "durationString": raw.get("duration_string"),
        "description": raw.get("description"),
        "viewCount": raw.get("view_count"),
        "uploadDate": raw.get("upload_date"),
        "extractor": raw.get("extractor_key") or raw.get("extractor"),
        "ext": raw.get("ext"),
        "formatCount": len(formats),
        "formats": formats[:120],
    }

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
            "yt-dlp", "--no-playlist", "--newline", "--progress-template", "download:%(progress.status)s|%(progress.percent)s|%(progress.downloaded_bytes)s|%(progress.total_bytes)s|%(progress.speed)s|%(progress.eta)s",
            "--continue", "--no-overwrites", "--no-cache-dir",
            "--retries", "10", "--fragment-retries", "10",
            "--retry-sleep", "http:linear=1::2",
            "--socket-timeout", "30",
            "--restrict-filenames",
            "--print", "after_move:filepath",
            "-o", out_tpl,
        ]
        requested_format = str(m.get("format") or "best").lower()
        if m.get("audio_only"):
            cmd += ["-x", "--audio-format", "mp3", "--audio-quality", "192K"]
        elif requested_format in {"best", "original"}:
            cmd += ["-f", "bv*+ba/b", "--merge-output-format", "mp4"]
        else:
            cmd += ["-f", requested_format]
        cmd += [m["url"]]

        log.info("job=%s start url=%s audio_only=%s format=%s", job_id, m.get("url"), m.get("audio_only"), m.get("format"))
        log.info("job=%s command=%s", job_id, " ".join(cmd[:-1]) + " <url>")
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
            if line:
                log.info("job=%s yt-dlp: %s", job_id, line)
            pct, total, speed, eta, downloaded = parse_progress(line)
            with lock:
                m = jobs.get(job_id)
                if not m:
                    break
                if pct is not None:
                    m["progress"] = pct
                if total:
                    m["totalBytes"] = total
                if speed is not None:
                    m["speedBps"] = speed
                if eta is not None:
                    m["etaSeconds"] = eta
                if downloaded is not None:
                    m["bytes"] = downloaded
                # Inspect the growing part/final file for byte progress.
                candidates = list(d.glob("*"))
                files = [p for p in candidates if p.is_file() and p.name != "meta.json"]
                if files:
                    largest = max(files, key=lambda p: p.stat().st_size)
                    try:
                        file_bytes = largest.stat().st_size
                        m["bytes"] = file_bytes
                        if not m.get("filename"):
                            m["filename"] = largest.name
                    except OSError:
                        pass
                if m.get("totalBytes") and m.get("progress") is not None:
                    m["bytes"] = max(int(m.get("bytes") or 0), int(m["totalBytes"] * float(m["progress"]) / 100))
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
                log.error("job=%s failed rc=%s", job_id, rc)
                m["updatedAt"] = now()
                write_meta(m)
                return

            final_path = final_path if final_path and final_path.exists() else find_output(job_id)
            if not final_path:
                m["status"] = "failed"
                m["error"] = "Download completed but output file was not found"
                log.error("job=%s output file missing", job_id)
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
        log.exception("job=%s crashed", job_id)
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
        log.info("request GET %s", self.path)
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
        log.info("request HEAD %s", self.path)
        if not self.require_auth():
            return
        parts = [x for x in urlparse(self.path).path.split("/") if x]
        if len(parts) == 4 and parts[0] == "v1" and parts[1] == "jobs" and parts[3] == "file":
            self.serve_file(parts[2], head_only=True)
            return
        self.send_response(404); self.end_headers()

    def do_POST(self):
        log.info("request POST %s", self.path)
        if not self.require_auth():
            return
        parts = [x for x in urlparse(self.path).path.split("/") if x]
        try:
            body = json_body(self)
        except Exception as e:
            self.send_json(400, {"error": str(e)})
            return

        if parts == ["v1", "inspect"]:
            url = str(body.get("url", "")).strip()
            if not re.match(r"^https?://", url, re.I):
                self.send_json(400, {"error": "url must be http/https"})
                return
            try:
                data = inspect_url(url)
                self.send_json(200, data)
            except subprocess.TimeoutExpired:
                self.send_json(504, {"error": "Media inspection timed out"})
            except Exception as e:
                log.error("inspect failed url=%s error=%s", url, e)
                self.send_json(422, {"error": str(e) or "Unable to inspect media URL"})
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
                "filename": None, "extension": None, "mime": None, "pid": None, "format": str(body.get("format") or "best"),
            }
            with lock:
                jobs[job_id] = m
                write_meta(m)
            log.info("job=%s created", job_id)
            run_job(job_id)
            self.send_json(202, enrich(m))
            return

        if len(parts) == 4 and parts[0] == "v1" and parts[1] == "jobs":
            job_id, action = parts[2], parts[3]
            with lock:
                m = jobs.get(job_id)
            if not m:
                self.send_json(404, {"error": "job not found"}); return
            log.info("job=%s action=%s status=%s", job_id, action, m.get("status"))
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
