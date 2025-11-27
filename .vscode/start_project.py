import csv
import os
import psycopg2

FOLDER_PATH = "D:/dbt_CaseStudy/raw_data/"        
SCHEMA_NAME = "datalake"         

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "mydb",
    "user": "postgres",
    "password": "postgres"
}


def quote_identifier(identifier: str) -> str:
    """
    Wrap column names in double-quotes to support reserved words.
    Also escape internal quotes by doubling them.
    """
    identifier = identifier.replace('"', '""')  # escape quotes
    return f'"{identifier}"'


def infer_data_type(value):
    if value == "" or value is None:
        return "TEXT"
    try:
        int(value)
        return "INTEGER"
    except:
        pass
    try:
        float(value)
        return "TEXT"
    except:
        pass
    if value.lower() in ["true", "false"]:
        return "BOOLEAN"
    return "TEXT"

def process_csv_file(csv_path, cursor):
    filename = os.path.splitext(os.path.basename(csv_path))[0].lower()
    table_name = f"{SCHEMA_NAME}.{filename}"

    print(f"\n Processing CSV: {csv_path}  →  table: {table_name}")

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        headers = next(reader)
        first_row = next(reader)

    # quote all column names for Postgres
    quoted_headers = [quote_identifier(h) for h in headers]

    # infer schema
    types = [infer_data_type(v) for v in first_row]
    schema = dict(zip(quoted_headers, types))

    for col, typ in schema.items():
        print(f"   {col}: {typ}")

    cols_sql = ", ".join([f"{col} {typ}" for col, typ in schema.items()])
    create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({cols_sql});"

    cursor.execute(create_table_sql)

    # Insert SQL
    placeholders = ", ".join(["%s"] * len(headers))
    insert_sql = f"INSERT INTO {table_name} ({', '.join(quoted_headers)}) VALUES ({placeholders});"

    # Insert data
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        for row in reader:
            cursor.execute(insert_sql, row)

    print(f"Finished loading '{table_name}'.")


# -------------------------------------------------------------
# Main — process all CSVs in folder
# -------------------------------------------------------------
def import_all_csvs():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # ensure schema exists
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};")
    cursor.execute(f"SET search_path TO {SCHEMA_NAME};")

    files = [f for f in os.listdir(FOLDER_PATH) if f.lower().endswith(".csv")]

    if not files:
        print("No CSV files found in folder!")
        return

    for file in files:
        process_csv_file(os.path.join(FOLDER_PATH, file), cursor)
        conn.commit()

    cursor.close()
    conn.close()
    print("\n ALL CSV FILES IMPORTED SUCCESSFULLY!")


if __name__ == "__main__":
    import_all_csvs()
