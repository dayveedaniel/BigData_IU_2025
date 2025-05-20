#!/usr/bin/env bash
set -euo pipefail

WAREHOUSE="/user/team5/project/warehouse" 
OUT_DIR="./output"                              
PASSWORD="$(< secrets/.psql.pass)"          

#  Clean previous run artefacts
if [[ -d "$OUT_DIR" ]]; then
  echo "Removing previous $OUT_DIR directory…"
  rm -rf "$OUT_DIR"
fi

if hadoop fs -test -d "$WAREHOUSE"; then
  echo "Removing previous HDFS warehouse $WAREHOUSE …"
  # ‑skipTrash avoids filling Trash; drop it if your admins disallow it
  hadoop fs -rm -r -skipTrash "$WAREHOUSE"
fi

# Re‑create local output dir
mkdir -p "$OUT_DIR"

#  Single import: Avro format + bzip2 compression

echo "▶ Importing all tables as Avro (codec=bzip2)…"
start=$(date +%s)

sqoop import-all-tables \
  --connect "jdbc:postgresql://hadoop-04.uni.innopolis.ru/team5_projectdb" \
  --username team5 \
  --password "$PASSWORD" \
  --compress \
  --compression-codec bzip2 \
  --as-avrodatafile \
  --warehouse-dir "$WAREHOUSE/avro" \
  --null-string '\\N' \
  --null-non-string '\\N' \
  --num-mappers 1 \
  --outdir "$OUT_DIR/src"

end=$(date +%s)
echo "✓ Import finished in $((end-start)) s"