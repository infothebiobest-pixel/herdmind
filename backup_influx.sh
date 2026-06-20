#!/bin/bash
BACKUP_DIR="$HOME/Dairy/HerdMind-X/backups/influx"
mkdir -p "$BACKUP_DIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
echo "⏳ Extracting raw engine data structures (token-free path)..."
docker cp herd_influx:/var/lib/influxdb2 "$BACKUP_DIR/$TIMESTAMP"
echo "✅ Database states archived perfectly to $BACKUP_DIR/$TIMESTAMP"
