#!/usr/bin/env bash
set -euo pipefail

bash scripts/export_avro.sh
bash scripts/create_hive_tables.sh
bash scripts/export_insights.sh