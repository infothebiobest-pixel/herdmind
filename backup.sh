#!/usr/bin/env bash

# Exit immediately if any command fails
set -e

PROJECT_DIR="$HOME/Dairy"
TARGET_DIR="$PROJECT_DIR/HerdMind-X"
WINDOWS_DESKTOP="/mnt/c/Users/hp/Desktop"
BACKUP_NAME="HerdMind-X-backup-$(date +%Y%m%d_%H%M).zip"

echo "📦 Starting HerdMind-X optimization backup..."

# Ensure we are running from the parent folder
cd "$PROJECT_DIR"

# Generate the compressed archive with robust folder exclusions
# Update the zip line in your file to look exactly like this:
zip -r "$BACKUP_NAME" HerdMind-X/ -x "*/.venv/*" "*/infra/grafana/*" "*/infra/influxdb/*" "*/data/*" "*.git*" > /dev/null


# Verify Windows desktop path exists before copying
if [ -d "$WINDOWS_DESKTOP" ]; then
    cp "$BACKUP_NAME" "$WINDOWS_DESKTOP/"
    echo "📋 Successfully mirrored backup to Windows Desktop!"
else
    echo "⚠️ Windows Desktop path not found. Backup kept locally in Linux."
fi

# Print storage utilization breakdown
echo "🏁 Backup completed successfully!"
ls -lh "$BACKUP_NAME"
