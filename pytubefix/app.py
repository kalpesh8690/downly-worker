import os
import re
import shutil
import subprocess
import threading
import uuid
from pathlib import Path
from urllib.parse import urlparse

from flask import Flask, jsonify, redirect, request, send_file
from flask_cors import CORS
from pytubefix import YouTube

BASE_DIR = Path(__file__).resolve().parent
DOWNLOAD_DIR = BASE_DIR / "downloads"
TEMP_DIR = BASE_DIR / "temp"
DOWNLOAD_DIR.mkdir(exist_ok=True)
TEMP_DIR.mkdir(exist_ok=True)

PORT = int(os.getenv("PORT", "5000"))
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})

jobs = {}
jobs_lock = threading.Lock()


def valid_youtube_url(url):
    if not url:
        return False
    try:
        parsed = urlparse(url)
        host = parsed.netloc.lower()
        allowed = {
            "youtube.com",
            "www.youtube.com",
            "m.youtube.com",
            "youtu.be",
            "www.youtu.be",
            "youtube-nocookie.com",
            "www.youtube-nocookie.com",
        }
        return (
            parsed.scheme in ("http", "https")
            and any(host == d or host.endswith("." + d) for d in allowed)
        )
    except Exception:
        return False


def safe_filename(name):
    name = re.sub(r'[\\/*?:"<>|]', "", name or "")
    name = re.sub(r"\s+", " ", name).strip()
    return (name or "video")[:180]


def size_info(stream):
    size = getattr(stream, "filesize", None) or getattr(stream, "filesize_approx", None)
    if not size:
        return {"bytes": None, "mb": None, "gb": None, "display": "Unknown"}
    mb = round(size / 1024 / 1024, 2)
    gb = round(size / 1024 / 1024 / 1024, 2)
    return {
        "bytes": size,
        "mb": mb,
        "gb": gb,
        "display": f"{gb} GB" if gb >= 1 else f"{mb} MB",
    }


def stream_json(stream):
    video_codec = getattr(stream, "video_codec", None)
    audio_codec = getattr(stream, "audio_codec", None)
    if video_codec and audio_codec:
        kind = "video_audio"
    elif video_codec:
        kind = "video"
    elif audio_codec:
        kind = "audio"
    else:
        kind = "unknown"

    return {
        "itag": stream.itag,
        "type": kind,
        "mime_type": getattr(stream, "mime_type", None),
        "extension": getattr(stream, "subtype", None),
        "resolution": getattr(stream, "resolution", None),
        "width": getattr(stream, "width", None),
        "height": getattr(stream, "height", None),
        "fps": getattr(stream, "fps", None),
        "video_codec": video_codec,
        "audio_codec": audio_codec,
        "progressive": bool(getattr(stream, "is_progressive", False)),
        "adaptive": bool(getattr(stream, "is_adaptive", False)),
        "abr": getattr(stream, "abr", None),
        "bitrate": getattr(stream, "bitrate", None),
        "size": size_info(stream),
        "includes_audio": bool(audio_codec),
        "includes_video": bool(video_codec),
    }


def update_job(job_id, **values):
    with jobs_lock:
        if job_id in jobs:
            jobs[job_id].update(values)


def progress_callback(job_id):
    def callback(stream, chunk, bytes_remaining):
        total = getattr(stream, "filesize", None) or getattr(stream, "filesize_approx", None)
        if total:
            downloaded = max(0, total - bytes_remaining)
            update_job(
                job_id,
                progress=round(downloaded / total * 100, 2),
                downloaded=downloaded,
                total=total,
            )
    return callback


def run_ffmpeg(args):
    subprocess.run(
        ["ffmpeg", "-y", *args],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def worker(job_id, url, itag, mode, output_format):
    work_dir = TEMP_DIR / job_id
    work_dir.mkdir(parents=True, exist_ok=True)

    try:
        update_job(job_id, status="starting", progress=0)
        yt = YouTube(url, on_progress_callback=progress_callback(job_id))
        update_job(
            job_id,
            status="downloading",
            title=yt.title,
            thumbnail=yt.thumbnail_url,
        )

        selected = yt.streams.get_by_itag(int(itag))
        if not selected:
            raise RuntimeError("Format not found")

        if mode == "audio":
            source = selected.download(output_path=str(work_dir))
            update_job(job_id, status="converting", progress=90)

            ext = output_format.lower()
            if ext not in {"mp3", "m4a", "wav"}:
                raise RuntimeError("Unsupported audio format")

            output = DOWNLOAD_DIR / f"{job_id}_{safe_filename(yt.title)}.{ext}"

            if ext == "mp3":
                run_ffmpeg(["-i", source, "-vn", "-codec:a", "libmp3lame", "-b:a", "192k", str(output)])
            elif ext == "m4a":
                run_ffmpeg(["-i", source, "-vn", "-c:a", "aac", "-b:a", "192k", str(output)])
            else:
                run_ffmpeg(["-i", source, "-vn", str(output)])

        elif selected.is_progressive:
            source = selected.download(output_path=str(work_dir))
            output = DOWNLOAD_DIR / f"{job_id}_{safe_filename(yt.title)}.{selected.subtype}"
            shutil.move(source, output)

        else:
            video_file = selected.download(output_path=str(work_dir))
            update_job(job_id, status="downloading_audio", progress=50)

            audio_stream = (
                yt.streams
                .filter(only_audio=True)
                .order_by("abr")
                .desc()
                .first()
            )
            if not audio_stream:
                raise RuntimeError("Compatible audio stream not found")

            audio_file = audio_stream.download(output_path=str(work_dir))
            update_job(job_id, status="merging", progress=90)

            output = DOWNLOAD_DIR / f"{job_id}_{safe_filename(yt.title)}.mp4"
            run_ffmpeg([
                "-i", video_file,
                "-i", audio_file,
                "-map", "0:v:0",
                "-map", "1:a:0",
                "-c:v", "copy",
                "-c:a", "aac",
                "-movflags", "+faststart",
                str(output),
            ])
        else:
            raise RuntimeError("Invalid mode")

        update_job(
            job_id,
            status="completed",
            progress=100,
            file=str(output),
            filename=output.name,
        )
    except Exception as exc:
        update_job(job_id, status="error", error=str(exc))
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


@app.get("/")
def root():
    return jsonify({
        "success": True,
        "name": "YouTube Downloader API",
        "engine": "pytubefix",
        "platform": "Termux",
    })


@app.get("/api/health")
def health():
    return jsonify({
        "success": True,
        "ffmpeg": shutil.which("ffmpeg") is not None,
        "ffmpeg_path": shutil.which("ffmpeg"),
    })


@app.post("/api/info")
def info():
    data = request.get_json(silent=True) or {}
    url = data.get("url")
    if not valid_youtube_url(url):
        return jsonify({"success": False, "error": "Invalid YouTube URL"}), 400

    try:
        yt = YouTube(url)
        formats = []
        for stream in yt.streams:
            try:
                formats.append(stream_json(stream))
            except Exception:
                pass

        formats.sort(
            key=lambda x: (
                x.get("height") or 0,
                x.get("fps") or 0,
                x.get("size", {}).get("bytes") or 0,
            ),
            reverse=True,
        )

        return jsonify({
            "success": True,
            "video": {
                "title": yt.title,
                "author": yt.author,
                "duration": yt.length,
                "thumbnail": yt.thumbnail_url,
                "views": getattr(yt, "views", None),
                "watch_url": url,
            },
            "formats": formats,
            "total_formats": len(formats),
        })
    except Exception as exc:
        return jsonify({"success": False, "error": str(exc)}), 500


@app.post("/api/preview")
def preview():
    data = request.get_json(silent=True) or {}
    url, itag = data.get("url"), data.get("itag")
    if not valid_youtube_url(url) or not itag:
        return jsonify({"success": False, "error": "url and itag are required"}), 400

    try:
        yt = YouTube(url)
        stream = yt.streams.get_by_itag(int(itag))
        if not stream:
            return jsonify({"success": False, "error": "Format not found"}), 404

        return jsonify({
            "success": True,
            "preview": {
                "url": stream.url,
                "itag": stream.itag,
                "mime_type": stream.mime_type,
                "extension": stream.subtype,
                "has_audio": bool(getattr(stream, "audio_codec", None)),
                "has_video": bool(getattr(stream, "video_codec", None)),
            },
        })
    except Exception as exc:
        return jsonify({"success": False, "error": str(exc)}), 500


@app.get("/api/preview/<int:itag>")
def preview_redirect(itag):
    url = request.args.get("url")
    if not valid_youtube_url(url):
        return jsonify({"success": False, "error": "Invalid YouTube URL"}), 400

    try:
        stream = YouTube(url).streams.get_by_itag(itag)
        if not stream:
            return jsonify({"success": False, "error": "Format not found"}), 404
        return redirect(stream.url, code=302)
    except Exception as exc:
        return jsonify({"success": False, "error": str(exc)}), 500


@app.post("/api/download")
def download():
    data = request.get_json(silent=True) or {}
    url = data.get("url")
    itag = data.get("itag")
    mode = data.get("mode", "video")
    output_format = data.get("output_format", "mp3")

    if not valid_youtube_url(url) or not itag:
        return jsonify({"success": False, "error": "url and itag are required"}), 400
    if mode not in {"video", "audio"}:
        return jsonify({"success": False, "error": "Invalid mode"}), 400
    if mode == "audio" and output_format not in {"mp3", "m4a", "wav"}:
        return jsonify({"success": False, "error": "Invalid audio format"}), 400

    job_id = str(uuid.uuid4())
    with jobs_lock:
        jobs[job_id] = {
            "id": job_id,
            "status": "queued",
            "progress": 0,
            "downloaded": 0,
            "total": 0,
            "file": None,
            "filename": None,
            "error": None,
        }

    threading.Thread(
        target=worker,
        args=(job_id, url, itag, mode, output_format),
        daemon=True,
    ).start()

    return jsonify({
        "success": True,
        "job_id": job_id,
        "status": "queued",
        "progress_url": f"/api/progress/{job_id}",
        "file_url": f"/api/file/{job_id}",
    })


@app.get("/api/progress/<job_id>")
def progress(job_id):
    with jobs_lock:
        job = jobs.get(job_id)
        if not job:
            return jsonify({"success": False, "error": "Job not found"}), 404
        return jsonify({"success": True, "job": job})


@app.get("/api/file/<job_id>")
def file(job_id):
    with jobs_lock:
        job = jobs.get(job_id)
        if not job:
            return jsonify({"success": False, "error": "Job not found"}), 404
        if job["status"] != "completed":
            return jsonify({
                "success": False,
                "error": "File is not ready",
                "status": job["status"],
            }), 409
        path, filename = job["file"], job["filename"]

    if not path or not os.path.exists(path):
        return jsonify({"success": False, "error": "File no longer exists"}), 404

    return send_file(path, as_attachment=True, download_name=filename)


@app.delete("/api/job/<job_id>")
def delete_job(job_id):
    with jobs_lock:
        job = jobs.pop(job_id, None)
    if not job:
        return jsonify({"success": False, "error": "Job not found"}), 404

    path = job.get("file")
    if path and os.path.exists(path):
        try:
            os.remove(path)
        except OSError:
            pass

    return jsonify({"success": True})


if __name__ == "__main__":
    print(f"YouTube Downloader API listening on 0.0.0.0:{PORT}")
    app.run(host="0.0.0.0", port=PORT, threaded=True, debug=False)
