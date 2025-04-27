#!/bin/bash

# Set strict mode for safer execution
set -euo pipefail

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "Starting data pipeline..."

# Run data collection script
echo "Running data collection..."
"$SCRIPT_DIR/scripts/data_collection.sh"

# Run data storage script
echo "Running data storage..."
"$SCRIPT_DIR/scripts/data_storage.sh"

echo "Data pipeline completed successfully!"
