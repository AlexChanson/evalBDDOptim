
def loadWorkload(path):
    data = ""
    with open(path) as f:
        for line in f:
            if line.startswith("--"):
                pass
            else:
                data += " " + line.strip()
    return data.split(";")[:-1]

def run_explain_analyze(query, connection):
    explain_query = f"EXPLAIN ANALYZE {query}"
    try:
        with connection.cursor() as cur:
            cur.execute(explain_query)
            result = cur.fetchall()
            return result
    except Exception as e:
        print(f"Error running EXPLAIN ANALYZE: {e}")
        return None

def run_create(query, connection):
    try:
        with connection.cursor() as cur:
            result = cur.execute(query)
            connection.commit()
            return result
    except Exception as e:
        print(f"Error running CREATE: {e}")
        return None

import re

def extract_table_name(create_table_sql):
    """
    Extracts the table name from a CREATE TABLE SQL statement.

    Parameters:
    - create_table_sql (str): The CREATE TABLE SQL statement.

    Returns:
    - str: The extracted table name, or None if not found.
    """
    # Normalize and strip leading/trailing whitespace
    sql = create_table_sql.strip().upper()

    # Match pattern: CREATE [TEMP|TEMPORARY] TABLE [IF NOT EXISTS] schema.table (or just table)
    match = re.search(
        r'CREATE\s+(?:TEMP|TEMPORARY\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([\w\."]+)',
        create_table_sql,
        re.IGNORECASE
    )

    if match:
        raw_table_name = match.group(1)
        # Remove surrounding quotes if any
        table_name = raw_table_name.strip('"')
        return table_name
    return None

import psycopg2

def import_csv_to_table(table_name, csv_file_path, connection, delimiter=',', header=True):
    """
    Imports data from a CSV file into a PostgreSQL table using psycopg2.

    Parameters:
    - table_name (str): The name of the target table.
    - csv_file_path (str): The path to the CSV file.
    - connection_params (dict): Dictionary of connection params for psycopg2.
    - delimiter (str): CSV delimiter, default is ','.
    - header (bool): True if CSV file has a header row, False otherwise.

    Returns:
    - None
    """
    try:

        with connection.cursor() as cur:
            with open(csv_file_path, 'r') as f:
                copy_sql = f"""
                COPY {table_name}
                FROM STDIN WITH CSV {f'HEADER' if header else ''} DELIMITER '{delimiter}'
                """
                cur.copy_expert(copy_sql, f)
                print(f"Data imported successfully into '{table_name}'.")
    except Exception as e:
        print(f"Error importing data into table '{table_name}': {e}")

def analyze_table(table_name, connection):
    try:
        with connection.cursor() as cur:
            sql = f"""
            ANALYZE {table_name};
            """
            cur.execute(sql)
            print(f"Successfully analyzed '{table_name}'.")
    except Exception as e:
        print(f"Analysis error fo '{table_name}': {e}")

def run_arbitrary(sql, connection):
    try:
        with connection.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()
    except Exception as e:
        print(f"Error running query: {e}")