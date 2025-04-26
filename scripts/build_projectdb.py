import psycopg2 as psql
from pprint import pprint
import os

# Read password from secrets file
file = os.path.join("secrets", ".psql.pass")
with open(file, "r") as file:
    password = file.read().rstrip()

# df = pd.read_csv('data/US_Accidents_March23.csv')

# build connection string
conn_string = "host=hadoop-01.uni.innopolis.ru port=22 user=team5 dbname=team5_projectdb password={}".format(password)

# Connect to the remote dbms
with psql.connect(conn_string) as conn:
    # Create a cursor for executing psql commands
    cur = conn.cursor()
    
    # Read and execute create tables commands
    with open(os.path.join("sql", "create_tables.sql")) as file:
        content = file.read()
        cur.execute(content)
    conn.commit()

    # Read and execute import data commands
    with open(os.path.join("sql", "import_data.sql")) as file:
        commands = file.readlines()
        # Import the cleaned accident data
        with open(os.path.join("data", "US_Accidents_March23_cleaned.csv"), "r") as accidents:
            cur.copy_expert(commands[0], accidents)
    
    # Commit the changes
    conn.commit()
