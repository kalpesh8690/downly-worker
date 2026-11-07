#!/data/data/com.termux/files/usr/bin/bash
set -e

cd "$(dirname "$0")"

pkg install -y python ffmpeg

python -m pip install --upgrade pip
python -m pip install -r requirements.txt

mkdir -p downloads temp

echo "Starting pytubefix backend on port 5000..."
python app.py
