#!/usr/bin/env bash
set -euo pipefail

# ────────── constants ────────── #
ITERATIONS=5
TEST_DB="temp_team5_projectdb" # Hive database name

RESULTS_DIR="./output/benchmark"
RESULT_CSV="$RESULTS_DIR/result.csv"

BASE_WAREHOUSE="/user/team5/project/temp_warehouse" # HDFS base for Sqoop data
OUT_DIR="./temp_output" # Local temp dir for Sqoop --outdir, HQL scripts

HIVE_URI="jdbc:hive2://hadoop-03.uni.innopolis.ru:10001"
PASSWORD="$(< secrets/.psql.pass)"
HUSER="team5" # Your HDFS username

declare -A codec_map=(
  [avro]="snappy bzip2"
  [parquet]="snappy gzip"
)

# ────────── cleanup trap ────────── #
# Ensure local temporary files are cleaned up on exit
cleanup_local_temp() {
  echo "🧹 Cleaning up local temp directory $OUT_DIR ..." >&2
  rm -rf "$OUT_DIR"
}

trap cleanup_local_temp EXIT SIGINT SIGTERM


# ────────── clean environment ────────── #
echo "🔄 Cleaning previous runs …" >&2
rm -rf "$RESULTS_DIR" # Remove local results dir
mkdir -p "$RESULTS_DIR" # Create local results dir
mkdir -p "$OUT_DIR"     # Create local temp dir

if hadoop fs -test -d "$BASE_WAREHOUSE"; then
  echo "⚠️  Removing previous HDFS $BASE_WAREHOUSE …" >&2
  hadoop fs -rm -r -skipTrash "$BASE_WAREHOUSE"
fi
# No need to create $BASE_WAREHOUSE here, Sqoop will do it via --target-dir

# ────────── Prepare Avro Schema (Java class generation) ────────── #
echo "⏳ Generating Java class once for 'accidents' table using Sqoop codegen..." >&2
echo "   This class will be used by subsequent 'sqoop import' commands." >&2


sqoop codegen \
    --table accidents \
    --connect "jdbc:postgresql://hadoop-04.uni.innopolis.ru/team5_projectdb" \
    --username team5 --password "$PASSWORD" \
    --outdir "$OUT_DIR/src" \
    >/dev/null 2>&1

if [[ ! -f "$OUT_DIR/src/accidents.java" ]]; then
  echo "❌ ERROR: accidents.java not found in $OUT_DIR/src after sqoop codegen." >&2
  echo "Ensure the 'accidents' table exists in PostgreSQL and Sqoop can access it." >&2
  exit 1
else
  echo "✅ Java class for 'accidents' table generated successfully in $OUT_DIR/src/" >&2
fi


# Write CSV Header
echo "format,compression,iteration,size_mb,import_time_sec,read_sec" > "$RESULT_CSV"

# ────────── helpers ────────── #
sqoop_import() { # args: fmt comp iter
  local fmt=$1 comp=$2 iter=$3 fmt_flag
  case $fmt in
    avro)    fmt_flag="--as-avrodatafile" ;;
    parquet) fmt_flag="--as-parquetfile"  ;;
    *) echo "unknown fmt $fmt" >&2 ; exit 1 ;;
  esac
  # Target HDFS directory for Sqoop import, includes format and compression
  local tgt_sqoop_dir="$BASE_WAREHOUSE/${fmt}_${comp}/accidents_data_iter${iter}"
  # Ensure target dir for this specific iteration is clean, Sqoop requires target-dir not to exist
  if hadoop fs -test -d "$tgt_sqoop_dir"; then
      echo "   ⚠️  Removing previous HDFS import dir $tgt_sqoop_dir for this iteration..." >&2
      hadoop fs -rm -r -skipTrash "$tgt_sqoop_dir"
  fi

  local start_time
  start_time=$(date +%s)

  echo "   📤 Sqoop import: $fmt ($comp), iter $iter to $tgt_sqoop_dir" >&2
  sqoop import \
      --table accidents \
      --connect "jdbc:postgresql://hadoop-04.uni.innopolis.ru/team5_projectdb" \
      --username team5 --password "$PASSWORD" \
      --compress --compression-codec "$comp" \
      $fmt_flag \
      --target-dir "$tgt_sqoop_dir" \
      --null-string '\\N' --null-non-string '\\N' \
      --num-mappers 1 \
      --outdir "$OUT_DIR/src" \
      --delete-target-dir \
      >/dev/null # Suppress Sqoop's stdout, errors will still go to stderr

  local end_time
  end_time=$(date +%s)
  echo $((end_time - start_time)) # This is the captured import time
}
hive_read() { # args: fmt comp iter
 local fmt=$1 comp=$2 iter=$3
  # HDFS location of the data imported by Sqoop for this specific iteration
  local data_loc_hdfs="$BASE_WAREHOUSE/${fmt}_${comp}/accidents_data_iter${iter}"
  # Unique Hive table name for this test run
  local hive_tbl_name="acc_${fmt}_${comp}_iter${iter}_tmp"
  # HDFS location for Hive DATABASE metadata (not table data for EXTERNAL tables)
  local hive_db_hdfs_location="/user/${HUSER}/project/hive/temp_warehouse_db" # Full path

  echo "    Dropping Hive DB $TEST_DB if exists..." >&2
  beeline -u "$HIVE_URI" -n "$HUSER" -p "$PASSWORD" \
    -e "DROP DATABASE IF EXISTS $TEST_DB CASCADE;" >/dev/null 2>&1
  echo "   Creating Hive DB $TEST_DB LOCATION '$hive_db_hdfs_location'..." >&2
  beeline -u "$HIVE_URI" -n "$HUSER" -p "$PASSWORD" \
    -e "CREATE DATABASE $TEST_DB LOCATION '$hive_db_hdfs_location';" >/dev/null 2>&1 # Errors will show

  local hql_content
  if [[ $fmt == "avro" ]]; then
    read -r -d '' hql_content <<EOF
USE $TEST_DB;
DROP TABLE IF EXISTS $hive_tbl_name;
CREATE EXTERNAL TABLE $hive_tbl_name
STORED AS AVRO
LOCATION '$data_loc_hdfs'
TBLPROPERTIES (
  'avro.output.codec'='$comp'
);
SELECT COUNT(*) FROM $hive_tbl_name;
EOF

  else # parquet
    read -r -d '' hql_content <<EOF
USE $TEST_DB;
DROP TABLE IF EXISTS $hive_tbl_name;
CREATE EXTERNAL TABLE $hive_tbl_name (
  id STRING, source STRING, severity SMALLINT, start_time TIMESTAMP,
  end_time TIMESTAMP, distance_mi DOUBLE, description STRING,
  location_id BIGINT, weather_id BIGINT, twilight_id SMALLINT,
  road_feat_id SMALLINT
)
STORED AS PARQUET
LOCATION '$data_loc_hdfs'
TBLPROPERTIES ('parquet.compression'='$comp');
SELECT COUNT(*) FROM $hive_tbl_name;
EOF
  fi

  local hql_temp_file
  hql_temp_file=$(mktemp -p "$OUT_DIR" "hive_query_XXXXXX.hql")

  echo "$hql_content" > "$hql_temp_file"

  echo "   ⚙️  Hive read: $fmt ($comp), iter $iter. Query in $hql_temp_file" >&2

  local captured_time_and_beeline_stderr

  captured_time_and_beeline_stderr=$( { \
    /usr/bin/time -f '%e' \
      beeline -u "$HIVE_URI" -n "$HUSER" -p "$PASSWORD" -f "$hql_temp_file" \
      1>/dev/null; \
    } 2>&1 )

  rm -f "$hql_temp_file" # Clean up temp HQL file

  # Extract the last line, which should be the time from /usr/bin/time.
  # Other lines might be error messages from Beeline if it failed.
  local final_time_value
  final_time_value=$(echo "$captured_time_and_beeline_stderr" | awk 'END{print}')

  if [[ "$final_time_value" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
    echo "$final_time_value"
  else
    echo "   ⚠️  WARNING: Hive read for $fmt/$comp iter $iter might have failed or produced non-numeric time." >&2
    echo "   Full output from time/beeline stderr: " >&2
    echo "$captured_time_and_beeline_stderr" >&2
    echo "0.00" # Return a placeholder numeric value for CSV
  fi
}

# ────────── main loop ────────── #
for iter in $(seq 1 "$ITERATIONS"); do
  echo -e "\n🏁  ITERATION $iter / $ITERATIONS" >&2
  for fmt in "${!codec_map[@]}"; do
    for comp in ${codec_map[$fmt]}; do
      echo "  🧪 Testing Format: $fmt, Compression: $comp (Iteration $iter)" >&2
      # Sqoop target dir for this specific iteration (data will be here)
      # This path needs to be consistent with what hive_read expects for data_loc_hdfs
      current_data_hdfs_path="$BASE_WAREHOUSE/${fmt}_${comp}/accidents_data_iter${iter}"

      write_time=$(sqoop_import "$fmt" "$comp" "$iter")
      read_time=$(hive_read "$fmt" "$comp" "$iter") # Pass iter for data path consistency

      # Get size of the HDFS directory for this specific iteration's import
      # Ensure this path matches where sqoop_import actually placed the data
      size_bytes=$(hadoop fs -du -s "$current_data_hdfs_path" | awk '{print $1}')
      size_mb=$(echo "scale=2; $size_bytes / (1024*1024)" | bc)

      echo "$fmt,$comp,$iter,$size_mb,$write_time,$read_time" >> "$RESULT_CSV"
      echo "    📊 Results: Size=$size_mb MB, ImportTime=$write_time s, ReadTime=$read_time s" >&2
    done
  done
done

# ────────── final teardown (HDFS) ────────── #
echo "🧹 Cleaning up HDFS base warehouse $BASE_WAREHOUSE ..." >&2
hadoop fs -rm -r -skipTrash "$BASE_WAREHOUSE" || true

# Local $OUT_DIR is cleaned by the trap

echo "✅  All done → $RESULT_CSV"