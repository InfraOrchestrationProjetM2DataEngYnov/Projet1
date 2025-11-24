#!/usr/bin/env bash
set -e

# Convert CRLF -> LF si présent
sed -i 's/\r$//' "$0"

echo "=== Entrypoint: Installing Python dependencies ==="

if [ -f /opt/hadoop/jobs/requirements.txt ]; then
  echo "Found requirements.txt, installing..."
  pip3 install --no-cache-dir --upgrade pip
  pip3 install --no-cache-dir -r /opt/hadoop/jobs/requirements.txt
  echo "Dependencies installed successfully"
else
  echo "WARNING: requirements.txt not found at /opt/hadoop/jobs/requirements.txt"
fi

echo "=== Entrypoint: Executing command ==="
exec "$@"
