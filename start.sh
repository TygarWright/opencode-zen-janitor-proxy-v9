#!/data/data/com.termux/files/usr/bin/bash
set -e
cd "$(dirname "$0")"
pkg install python -y >/dev/null 2>&1 || true
python -m pip install -q --disable-pip-version-check -r requirements.txt
python -m py_compile server.py
exec python server.py
