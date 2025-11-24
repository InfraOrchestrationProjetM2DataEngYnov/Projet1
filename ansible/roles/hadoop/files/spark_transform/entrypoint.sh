#!/bin/bash
set -e

echo "=== Entrypoint: Installing Python dependencies ==="

# Install Python deps from mounted /opt/hadoop/jobs/requirements.txt
if [ -f /opt/hadoop/jobs/requirements.txt ]; then
  echo "Found requirements.txt, installing..."
  pip3 install --no-cache-dir --upgrade pip
  pip3 install --no-cache-dir -r /opt/hadoop/jobs/requirements.txt
  echo "Dependencies installed successfully"
else
  echo "WARNING: requirements.txt not found at /opt/hadoop/jobs/requirements.txt"
fi

echo "=== Entrypoint: Executing command ==="
# Run the real command (spark-submit + args)
exec "$@"