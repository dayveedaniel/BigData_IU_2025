#!/bin/bash

# Set strict mode for safer execution
set -euo pipefail

# Set paths relative to the script location
PROJECT_DIR=$(dirname "$(pwd)")
DATA_DIR="$PROJECT_DIR/data"
ZIP_FILE="$DATA_DIR/us-accidents.zip"
CSV_FILE="$DATA_DIR/US_Accidents_March23.csv"

# Create data directory if it doesn't exist
mkdir -p "$DATA_DIR"

# Check if the CSV file already exists
if [[ -f "$CSV_FILE" ]]; then
    echo "[INFO] CSV already exists at $CSV_FILE"
    echo "[INFO] Skipping download and extraction."
else
    echo "[INFO] Downloading dataset..."
    curl -L -o "$ZIP_FILE" https://www.kaggle.com/api/v1/datasets/download/sobhanmoosavi/us-accidents

    echo "[INFO] Unzipping dataset..."
    unzip "$ZIP_FILE" -d "$DATA_DIR"

    echo "[INFO] Cleaning up ZIP file..."
    rm "$ZIP_FILE"

    echo "[INFO] Dataset is ready in $DATA_DIR"
fi
