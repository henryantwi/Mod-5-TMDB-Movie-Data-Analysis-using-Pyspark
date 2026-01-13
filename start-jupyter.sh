#!/bin/bash
set -e

# Install requirements if the file exists (Runtime Sync)
if [ -f "/home/spark/requirements.txt" ]; then
    echo "Syncing requirements..."
    pip install --no-cache-dir -r /home/spark/requirements.txt
fi

# Start Jupyter Lab with proper config
echo "Starting Jupyter Lab..."
jupyter lab \
    --ip=0.0.0.0 \
    --port=8888 \
    --no-browser \
    --IdentityProvider.token='spark' \
    --notebook-dir=/home/spark/work
