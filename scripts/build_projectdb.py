import os
import psycopg2 as psql
import multiprocessing
import time


# ── CONFIG ─────────────────────────────────────────────────────────
DATA_CSV = os.path.join("data", "US_Accidents_March23.csv")

SQL_CREATE_CORE   = os.path.join("sql", "create_tables.sql")
SQL_CREATE_STG    = os.path.join("sql", "create_staging.sql")  # Use optimized staging script
SQL_IMPORT_LOAD   = os.path.join("sql", "import_data.sql")     # Use optimized import script

PASSWORD = open(os.path.join("secrets", ".psql.pass"), encoding="utf‑8").read().strip()

CONN_STR = (
    "host=hadoop-04.uni.innopolis.ru "
    "port=5432 user=team5 "
    f"dbname=team5_projectdb password={PASSWORD}"
)

# Calculate optimal work_mem based on system RAM
# Use about 25% of available system memory for database operations
system_memory_mb = int(os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES') / (1024.**2) * 0.5)
work_mem = min(max(256, system_memory_mb // 8), 1024)  # Between 256MB and 1GB

# Calculate optimal maintenance_work_mem
maintenance_work_mem = min(max(512, system_memory_mb // 4), 2048)  # Between 512MB and 2GB

# ‑‑ COPY statement - standard format ‑‑
COPY_FROM_STDIN = """
    COPY staging_raw
    FROM STDIN
    WITH (FORMAT csv, HEADER true, NULL '', ENCODING 'UTF8');
"""

# ── HELPER ─────────────────────────────────────────────────────────
def run_sql(cur, path: str) -> None:
    """Execute an .sql file with UTF‑8 encoding."""
    print(f"Running SQL from: {path}")
    start_time = time.time()
    
    with open(path, encoding="utf‑8") as f:
        cur.execute(f.read())
    
    elapsed = time.time() - start_time
    print(f"Completed in {elapsed:.2f} seconds")


# ── MAIN LOAD ──────────────────────────────────────────────────────
def main():
    print(f"Starting database load process...")
    start_time = time.time()
    
    with psql.connect(CONN_STR) as conn:
        conn.autocommit = False  # single transaction
        with conn.cursor() as cur:
            # 1. advanced performance tweaks
            print("Setting PostgreSQL performance parameters...")
            cur.execute("SET synchronous_commit = OFF;")
            cur.execute(f"SET work_mem = '{work_mem}MB';")
            cur.execute(f"SET maintenance_work_mem = '{maintenance_work_mem}MB';")
            cur.execute("SET effective_io_concurrency = 8;")
            cur.execute("SET max_parallel_workers_per_gather = 4;")
            cur.execute("SET enable_partitionwise_join = on;")
            cur.execute("SET random_page_cost = 1.1;")
            cur.execute("SET effective_cache_size = '4GB';")

            # 2. create dimension & fact tables
            run_sql(cur, SQL_CREATE_CORE)

            # 3. create UNLOGGED staging table
            run_sql(cur, SQL_CREATE_STG)

            # 4. bulk‑copy CSV into staging_raw with larger buffer
            print(f"Loading data from {DATA_CSV}...")
            copy_start = time.time()
            
            buffer_size = 16 * 1024 * 1024  # 16 MiB buffer (increased from original)
            with open(DATA_CSV, "r", buffering=buffer_size) as f:
                cur.copy_expert(COPY_FROM_STDIN, f)
            
            copy_elapsed = time.time() - copy_start
            print(f"Data copy completed in {copy_elapsed:.2f} seconds")

            # 5. give planner stats for joins that follow
            print("Analyzing staging table...")
            cur.execute("ANALYZE staging_raw;")

            # 6. populate dimension & fact tables, drop staging_raw
            run_sql(cur, SQL_IMPORT_LOAD)
            
            # 7. commit the transaction
        print("Committing transaction...")
        conn.commit()
    
    # 9. Final analyze to update statistics
    print("Running final ANALYZE on all tables...")
    with psql.connect(CONN_STR) as conn:
        with conn.cursor() as cur:
            cur.execute("ANALYZE;")
            
            # Show tables
            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                ORDER BY table_name;
                """
            )
            print("\nTables present after load:")
            for (tbl,) in cur.fetchall():
                print(f"  • {tbl}")
                
            # Show table sizes
            cur.execute("""
                SELECT 
                    relname AS table_name,
                    pg_size_pretty(pg_total_relation_size(relid)) AS total_size
                FROM pg_catalog.pg_statio_user_tables
                ORDER BY pg_total_relation_size(relid) DESC;
            """)
            print("\nTable sizes:")
            for table_name, size in cur.fetchall():
                print(f"  • {table_name}: {size}")
                
    total_elapsed = time.time() - start_time
    print(f"\nTotal process completed in {total_elapsed:.2f} seconds (approximately {total_elapsed/60:.2f} minutes)")


# Set optimal Python parallelism
cores = multiprocessing.cpu_count()
os.environ["PYTHONUNBUFFERED"] = "1"
print(f"System has {cores} CPU cores")

main()