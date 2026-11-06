# Downly Worker

Persistent Termux/Android media worker for Downly. It uses Python's standard library, yt-dlp and FFmpeg.

## Capabilities

- Inspect supported public media URLs and return metadata + available formats
- Crawl a bounded number of same-origin HTML pages for MP4, WebM, MP3, HLS (.m3u8) and DASH (.mpd) references
- Stream direct progressive source URLs byte-for-byte to the browser without a completed worker-side file
- Use yt-dlp for supported extractors and FFmpeg for MP3 conversion or stream muxing
- Keep the older persistent job API for controlled/diagnostic workflows
- Apply per-worker stream concurrency limits
- Validate public HTTP(S) destinations and reject private/local addresses by default
- Redact URL query strings from normal logs

## Requirements

    pkg update
    pkg install python ffmpeg -y
    pkg install python-yt-dlp

For sites that require a JavaScript runtime, install a yt-dlp-supported JS runtime and the required yt-dlp EJS components. Keep yt-dlp updated using the Termux package manager or its documented installation method.

Verify:

    python --version
    yt-dlp --version
    ffmpeg -version

## Run

    cd ~/downly-worker
    export WORKER_TOKEN='USE_A_LONG_RANDOM_SECRET'
    export HOST='127.0.0.1'
    export PORT='8080'
    export MAX_JOBS='2'
    export MAX_STREAMS='2'
    export DOWNLOAD_DIR="$HOME/downly-worker/downloads"
    python server.py

Keep the token private. Use the same token in the Termux API process as YTDLP_WORKER_TOKEN. Do not expose the worker port publicly; the Node Termux server is the browser-facing entry point.

Optional local-network access:

    export ALLOW_PRIVATE_URLS='true'

Private/LAN destinations are blocked by default to reduce SSRF exposure. Only enable this on a private, trusted installation when you intentionally need to download from your own LAN media server.

## API

All /v1 routes except GET /health require:

    Authorization: Bearer $WORKER_TOKEN

| Method | Route | Purpose |
|---|---|---|
| GET | /health | Worker liveness and concurrency |
| POST | /v1/inspect | Metadata and available format discovery |
| POST | /v1/crawl | Bounded same-origin media page crawl; body contains url |
| POST | /v1/stream | Live attachment stream; body contains url, optional direct_url, format, filename and audio_only |
| POST | /v1/jobs | Create a persistent job |
| GET | /v1/jobs/:id | Read job status |
| POST | /v1/jobs/:id/pause | Pause a persistent job |
| POST | /v1/jobs/:id/resume | Resume a persistent job |
| POST | /v1/jobs/:id/cancel | Cancel a persistent job |
| HEAD/GET | /v1/jobs/:id/file | Serve a completed persistent job file |

## Direct download behavior

The browser requests the same-origin Termux API. The worker either:

1. Proxies a single-file public source directly, preserving bytes and Range headers, or
2. Runs yt-dlp/FFmpeg and pipes the result to the browser response.

The streaming route does not save a completed media file first. MP3 requests are converted to MP3 while streaming when a compatible audio source is available. Video quality choices that require separate audio/video tracks use yt-dlp/FFmpeg and may take longer.

Chrome owns the native download progress after it receives the attachment response. The webpage does not buffer the entire download or claim to know Chrome's final transfer progress.

## Crawler boundaries

The crawler is intentionally bounded to six same-origin pages, a limited HTML response size per page, and a fixed media result limit. It recognizes public media references in common HTML attributes and embedded page configuration. It cannot guarantee extraction from JavaScript-only players, encrypted manifests, blob URLs, login-only content or protected media.

## Operational notes

- Keep WORKER_TOKEN and Cloudflare tunnel credentials out of Git.
- Prefer a named Cloudflare Tunnel for a stable hostname. Quick Tunnels are temporary.
- Monitor Termux worker logs for yt-dlp/FFmpeg failures.
- MAX_STREAMS limits simultaneous native streaming downloads.
- ALLOW_PRIVATE_URLS=true weakens SSRF protection; use only in a trusted local environment.
- Only process media you own or are authorized to download.
