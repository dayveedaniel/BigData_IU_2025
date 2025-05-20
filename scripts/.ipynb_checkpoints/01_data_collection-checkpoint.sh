#!/usr/bin/env bash
set -euo pipefail            # safer bash: exit on error, undefined vars, or broken pipes

DATA_DIR="./data"
ZIP_NAME="us-accidents.zip"
ZIP_URL="https://www.kaggle.com/api/v1/datasets/download/sobhanmoosavi/us-accidents"

# 1. Delete the old data directory if it exists
if [[ -d "$DATA_DIR" ]]; then
  echo "Removing existing $DATA_DIR ..."
  rm -rf "$DATA_DIR"
fi

# 2. Re-create a fresh data directory
mkdir -p "$DATA_DIR"

# 3. Download the ZIP file
echo "Downloading $ZIP_NAME ..."
curl -L -o "$DATA_DIR/$ZIP_NAME" "$ZIP_URL"

# 4. Unzip its contents into the data directory
echo "Unzipping $ZIP_NAME ..."
unzip -q "$DATA_DIR/$ZIP_NAME" -d "$DATA_DIR"

# 5. Remove the ZIP to save space
rm "$DATA_DIR/$ZIP_NAME"

echo "Done — dataset ready in $DATA_DIR/"