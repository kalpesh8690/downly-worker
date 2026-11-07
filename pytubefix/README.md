# Pytubefix worker for Downly

Standalone YouTube worker based on pytubefix for Termux or a Python host.

## Install

```bash
pkg update
pkg install python ffmpeg -y
cd ~/downly-worker/pytubefix
python -m pip install -r requirements.txt
```

## Run

```bash
export PORT=5000
python app.py
```

The worker exposes `/api/info`, `/api/preview`, `/api/preview/:itag`, `/api/download`, `/api/progress/:jobId`, `/api/file/:jobId`, and `/api/job/:jobId`.

This is kept as a standalone fallback worker so the existing Downly `server.py` API and Render deployment are not replaced.

Use only for media you own or are authorized to download.
