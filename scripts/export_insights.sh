#!/usr/bin/env bash
set -euo pipefail

pass=$(head -n 1 secrets/.psql.pass)

# Base output directories
LOCAL_OUTPUT_BASE_DIR="output"
HDFS_OUTPUT_BASE_DIR="/user/team5/project/hive/output" # Base HDFS path from your HQLs
LOG_DIR="logs"

mkdir -p "$LOCAL_OUTPUT_BASE_DIR"
mkdir -p "$LOG_DIR"

JDBC_URL="jdbc:hive2://hadoop-03.uni.innopolis.ru:10001/team5_projectdb"
HIVE_USER="team5"

echo "Starting HQL query processing and HDFS data retrieval..."

# Loop through all HQL files starting with 'q' and ending with '.hql' in the hive directory
for hql_full_path in hive/q*.hql; do
    if [[ ! -f "$hql_full_path" ]]; then
        echo "Skipping $hql_full_path (not a regular file)"
        continue
    fi

    hql_filename=$(basename "$hql_full_path")
    query_prefix="${hql_filename%%_*}" # Extracts "qN" from "qN_description.hql"

    # Define paths and filenames based on the query prefix
    hdfs_query_output_dir="${HDFS_OUTPUT_BASE_DIR}/${query_prefix}"
    local_csv_file="${LOCAL_OUTPUT_BASE_DIR}/${query_prefix}.csv"
    beeline_log_file="${LOG_DIR}/${query_prefix}_beeline.log"

    echo "------------------------------------"
    echo "Processing: $hql_filename -> $local_csv_file"
    echo "  (HDFS source: $hdfs_query_output_dir)"
    echo "------------------------------------"

    # 1. Run Beeline to execute the HQL (populates the HDFS external table location)
    echo "  Step 1: Running Beeline..."
    rm -f "$beeline_log_file" # Clear old log
    if ! beeline \
        -u "$JDBC_URL" \
        -n "$HIVE_USER" \
        -p "$pass" \
        -f "$hql_full_path" \
        --silent=true > "$beeline_log_file" 2>&1; then # Capture all beeline output

        echo "  ERROR: Beeline execution failed for $hql_filename."
        echo "  Please check the log: $beeline_log_file"
        # cat "$beeline_log_file" # Uncomment to print log contents on error
        continue # Skip to the next HQL file
    fi
    echo "  Beeline execution successful."

    # 2. Remove any old local CSV file before attempting to fetch new data
    rm -f "$local_csv_file"

    # 3. Fetch the data from HDFS and merge it into a single local CSV file
    echo "  Step 2: Fetching data from HDFS to $local_csv_file..."
    if hadoop fs -getmerge "$hdfs_query_output_dir" "$local_csv_file"; then
        # getmerge succeeded
        if [[ -s "$local_csv_file" ]]; then
            echo "  Successfully created CSV with data: $local_csv_file"
        else
            # getmerge succeeded, but the resulting file is empty.
            # This means the HDFS source directory was empty or contained only empty part-files.
            echo "  NOTE: Successfully created empty CSV: $local_csv_file (query likely returned no data or HDFS source was empty)."
        fi
    else
        # getmerge failed
        echo "  ERROR: 'hadoop fs -getmerge' failed for HDFS source $hdfs_query_output_dir."
        echo "  This could be because the HDFS directory does not exist, is empty (and getmerge treats this as error for some versions/configs),"
        echo "  or due to permissions/other HDFS issues. Review Beeline log: $beeline_log_file"
        echo "  Attempting to list HDFS source for debugging:"
        hadoop fs -ls "$hdfs_query_output_dir" || echo "  -> Failed to list HDFS directory (it might not exist or permissions issue)."
        # The $local_csv_file might be partially written or non-existent. rm -f at start handles cleanup for next run.
        continue # Skip to next HQL
    fi
    echo "" # Extra newline for readability
done

# A simple check if the loop ran at all
if ! ls hive/q*.hql > /dev/null 2>&1; then
    echo "WARNING: No HQL files matching 'hive/q*.hql' were found in the 'hive' directory."
fi

echo "All HQL query processing and data retrieval complete."