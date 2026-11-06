# Downly Worker

Persistent Termux worker for Downly using Python stdlib + yt-dlp + FFmpeg.

## Features

- Persistent job metadata
- Queue + configurable concurrency
- yt-dlp retries and fragment retries
- `.part`/continue support from yt-dlp
- Pause/resume/cancel process control
- Live progress, speed, bytes and ETA
- Accurate final filename, extension, MIME and size
- HTTP Range + HEAD file serving
- Atomic metadata writes
- Automatic cleanup of old completed/failed jobs
- Recovers safely after worker restart

## Install

```bash
pkg update
pkg install python ffmpeg -y
pkg install python-yt-dlp
```

Verify:

```yt-dlp --version```

## Run

```cd ~/downly-worker
export WORKER_TOKEN='CHANGE_ME_TO_A_LONG_RANDOM_SECRET'
export HOST='127.0.0.1'
export PORT='8080'
export MAX_JOBS='2'
export DOWNLOAD_DIR='$HOME/downly-worker/downloads'
python server.py
```

For the Downly Termux frontend, point `YTDLP_WORKER_URL` to `http://127.0.0.1:8080`.

## API

- `POST /v1/jobs` body: `{"url":"https://...","audio_only":false}`
- `GET /v1/jobs/:id`
- `POST /v1/jobs/:id/pause`
- `POST /v1/jobs/:id/resume`
- `POST /v1/jobs/:id/cancel`
- `HEAD /v1/jobs/:id/file`
- `GET /v1/jobs/:id/file` with optional HTTP Range

All /v1 endpoints require:

```
Authorization: Bearer $WORKER_TOKEN
```

## Important

Pause/resume uses OS process signals on the Termux process. The persisted yt-dlp partial download is retained so a future retry can continue when the source supports it. Browser-side IDM-style range resume remains separate: it resumes transfer of a completed worker file.

Use this only for media you are authorized to download and process.
