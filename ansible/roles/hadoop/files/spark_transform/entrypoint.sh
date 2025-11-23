#!/bin/sh
set -e

# Install Python deps from mounted /opt/hadoop/jobs/requirements.txt
if [ -f /opt/hadoop/jobs/requirements.txt ]; then
  pip3 install --no-cache-dir --upgrade pip
  pip3 install --no-cache-dir -r /opt/hadoop/jobs/requirements.txt
fi

# Run the real command (spark-submit + args)
exec "$@"
