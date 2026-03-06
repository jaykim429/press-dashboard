#!/usr/bin/env bash

set -euo pipefail

APP_DIR="${1:-$HOME/press-dashboard}"
USER_NAME="${2:-$(whoami)}"
GROUP_NAME="${3:-$USER_NAME}"

cd "$APP_DIR"
mkdir -p logs

if [ ! -x ".venv/bin/python" ]; then
  echo "[ERROR] Python venv not found at $APP_DIR/.venv"
  exit 1
fi

.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -r requirements.txt

chmod +x run_daily_ingest.sh run_dashboard.sh run_attachment_pipeline.sh

mkdir -p /tmp/press-systemd
for f in press-dashboard.service press-ingest.service press-ingest.timer; do
  sed \
    -e "s|/home/wjdgns429/press-dashboard|$APP_DIR|g" \
    -e "s|wjdgns429|$USER_NAME|g" \
    -e "s|Group=$USER_NAME|Group=$GROUP_NAME|g" \
    "deploy/systemd/$f" > "/tmp/press-systemd/$f"
done

sudo cp /tmp/press-systemd/press-dashboard.service /etc/systemd/system/
sudo cp /tmp/press-systemd/press-ingest.service /etc/systemd/system/
sudo cp /tmp/press-systemd/press-ingest.timer /etc/systemd/system/

sudo systemctl daemon-reload
sudo systemctl enable --now press-dashboard.service
sudo systemctl enable --now press-ingest.timer

echo "[INFO] Deployment completed."
echo "[INFO] Service status:"
sudo systemctl --no-pager --full status press-dashboard.service | head -40 || true
sudo systemctl --no-pager --full status press-ingest.timer | head -40 || true
