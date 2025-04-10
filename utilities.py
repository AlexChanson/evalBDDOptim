import sqlglot
import zipfile
import os
import re

def loadWorkload(path):
    data = ""
    with open(path) as f:
        for line in f:
            if line.startswith("--") or line.startswith("#"):
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


def run_optimisation(query, connection):
    try:
        with connection.cursor() as cur:
            result = cur.execute(query)
            connection.commit()
            return result
    except Exception as e:
        print(f"Error running student optimization: {e}")
        return None

def get_dbsize(dbname, connection):
    query='SELECT pg_size_pretty( pg_database_size(\''+dbname + '\') );'
    try:
        with connection.cursor() as cur:
            cur.execute(query)
            return cur.fetchall()[0][0]
    except Exception as e:
        print(f"Error getting DB size: {e}")
        return None



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

def import_csv_to_table_alt(table_name, csv_file_path, connection, delimiter=',', header=True):
    query="copy " + table_name + " from \'" + csv_file_path + "\' with (format csv,header);"
    execute_query(connection, query)


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


def split_sql_statements(sql_script: str):
    statements = []
    buffer = []
    in_do_block = False

    lines = sql_script.splitlines()

    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith('--') or stripped.startswith('#'):
            continue  # skip empty lines and comments

        # Detect start of DO block
        if stripped.startswith('DO $$'):
            in_do_block = True

        buffer.append(line)

        # Detect end of DO block
        if in_do_block and re.match(r'END\s*\$\$;?\s*$', stripped, re.IGNORECASE):
            statements.append('\n'.join(buffer).strip())
            buffer = []
            in_do_block = False
        elif not in_do_block and stripped.endswith(';'):
            statements.append('\n'.join(buffer).strip())
            buffer = []

    # Catch any trailing statements not properly closed
    if buffer:
        statements.append('\n'.join(buffer).strip())

    return statements

def is_valid_postgres_sql(statement: str) -> bool:
    # doesn't seem to handle cluster properly
    if statement.lower().lstrip().startswith("cluster"):
        return True
    try:
        # Use PostgreSQL dialect
        sqlglot.parse_one(statement, read='postgres')
        return True
    except sqlglot.errors.ParseError as e:
        print(f"Invalid SQL: {e}")
        return False



def explore_folder(root_path):
    # Make sure it's an absolute Unix-style path
    root_path = os.path.abspath(root_path)

    results = []

    for entry in os.scandir(root_path):
        if entry.is_dir():
            subfolder_name = entry.name
            subfolder_path = entry.path

            # Step 1: Get substring before '_'
            if '_' in subfolder_name:
                prefix = subfolder_name.split('_')[0]
            else:
                prefix = subfolder_name  # fallback if no '_'

            # Step 2: Get paths of 2 files in the subfolder
            files = [os.path.join(subfolder_path, f) for f in os.listdir(subfolder_path)
                     if os.path.isfile(os.path.join(subfolder_path, f))]

            if len(files) == 2:
                file1_path, file2_path = files
                results.append((prefix, file1_path, file2_path))
            else:
                print(f"Skipping '{subfolder_name}' - found {len(files)} files instead of 2")

    return results



def unzip_and_get_subfolder(zip_path):
    # Ensure the zip_path is absolute
    zip_path = os.path.abspath(zip_path)

    # Determine the extraction directory (same as zip file, no extension)
    extract_dir = zip_path.rstrip(".zip")

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)

    # Get list of directories in the extracted location
    subdirs = [os.path.join(extract_dir, name) for name in os.listdir(extract_dir)
               if os.path.isdir(os.path.join(extract_dir, name))]

    if len(subdirs) == 1:
        return subdirs[0]
    elif len(subdirs) == 0:
        raise ValueError(f"No subfolders found in extracted zip at {extract_dir}")
    else:
        raise ValueError(f"Multiple subfolders found in extracted zip at {extract_dir}: {subdirs}")


def execute_query(conn, query):
    """
    Executes a given SQL query using the established connection.

    :param conn: Connection object
    :param query: SQL query to be executed
    :return: Query result
    """
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        #print("Query executed successfully.")

        try:
            result = cursor.fetchall()
            return result
        except psycopg2.ProgrammingError:
            # If the query does not return any data (like an INSERT or UPDATE)
            return None
    except Exception as e:
        print(f"Error executing query: {e}")
        return None
    finally:
        cursor.close()

def dropAllIndex(conn, table):
    query="select indexname from pg_catalog.pg_indexes where tablename = \'" + table + "\';"
    res=execute_query(conn, query)
    for r in res:
        if not r[0].startswith('Key') and not r[0].endswith('_pkey'):
            drop="drop index \"" + r[0] + "\";"
            execute_query(conn, drop)

def close_connection(conn):
    """
    Closes the database connection.

    :param conn: Connection object
    """
    try:
        conn.close()
        print("Database connection closed.")
    except Exception as e:
        print(f"Error closing connection: {e}")


def getTableNames(conn):
    return execute_query(conn, "select tablename from pg_catalog.pg_tables where schemaname=\'public\';")

def dropAllTables(conn):
    tableames=getTableNames(conn)
    for n in tableames:
        ns=[str(i) for i in n]
        execute_query(conn, "drop table \""+ns[0]+"\";")


def dropDB(conn,dbname):
    return execute_query(conn, "drop database " + dbname + ";")

if __name__ == '__main__':
    # establish database connection
    connection_params = {
        'dbname': 'hackathon',
        'user': '',
        'password': '',
        'host': 'localhost',
        'port': 5432
    }
    connection_ok = False
    while not connection_ok:
        try:
            connection = psycopg2.connect(**connection_params)
            connection_ok = True
        except Exception as e:
            print(f"Error connecting to PostgreSQL: {e}")

    dropAllTables(connection)
