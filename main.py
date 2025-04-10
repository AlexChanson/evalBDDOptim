import os
from email import header
from subprocess import PIPE, Popen
import platform
from time import sleep
import re
import utilities
from random import shuffle as randomizeWorkload
import psycopg2
import docker

PG_VERSION = "14"
PG_PORT = 35432
PG_ADDRESS = "localhost"
RUN_ANALYZE = True
RUN_SOLUTION = True
WORKLOAD_RUNS = 5
VALIDATE_STATEMENTS = True

PATH_TO_ZIP = "/path/to/your/file.zip"  # Replace with your zip path


if __name__ == '__main__':
    # load queries in memory
    queries = utilities.loadWorkload("workload/queries.txt")
    tables = utilities.loadWorkload("workload/create.txt")
    solution = utilities.split_sql_statements(open("workload/student_setup.txt").read())
    solution_partition = utilities.loadWorkload("workload/student_create.txt")
    table_names = [utilities.extract_table_name(t).lstrip("public.") for t in tables]
    student_table_names = [utilities.extract_table_name(t).lstrip("public.") for t in solution_partition]
    print(len(tables), 'tables to create :', table_names)
    print(len(queries), 'queries to run.')

    # Extracting students answers
    try:
        subfolder_path = utilities.unzip_and_get_subfolder(PATH_TO_ZIP)
        print(f"[UNZIP] Extracted subfolder path: {subfolder_path}")
        data = utilities.explore_folder(subfolder_path)
        print(f"[INFO] Number of answers: {len(data)}")
        #for prefix, file1, file2 in data:
        #    print(f"Prefix: {prefix}")
        #    print(f"File 1: {file1}")
        #    print(f"File 2: {file2}")
        #    print("------")
    except Exception as e:
        print(f"Error in extracting from zip: {e}")


    if VALIDATE_STATEMENTS:
        for st in solution:
            if utilities.is_valid_postgres_sql(st):
                pass
            else:
                print("INVALID SQL ? :", st)
                exit(0)

    # Connect to the local docker daemon
    dd = docker.from_env()
    container = dd.containers.run("postgres:"+PG_VERSION, "-c random_page_cost=1.4 -c jit=off",name="evalOptimBDD", ports={"5432":PG_PORT},
                                  environment=["POSTGRES_PASSWORD=mysecretpassword","POSTGRES_USER=dbUser"],
                                   detach=True) #volumes=['tp_bdd_optim_data:/to_import']
    sleep(2)
    print("[DOCKER] Postgres instance is up and running")

    # establish database connection
    connection_params = {
         'dbname': 'dbUser',
         'user': 'dbUser',
         'password': 'mysecretpassword',
         'host': PG_ADDRESS,
         'port': PG_PORT
    }
    connection_ok = False
    while not connection_ok:
        try:
            connection = psycopg2.connect(**connection_params)
            connection_ok = True
        except Exception as e:
            print(f"Error connecting to PostgreSQL: {e}")
            sleep(5)

    # create tables
    #       student's
    for q in solution_partition :
        utilities.run_create(q, connection)
    #       default
    for i in range(len(tables)):
        query = tables[i]
        if table_names[i] not in student_table_names:
            res = utilities.run_create(query, connection)
    print("[PGSQL] tables created")

    # import data
    # import_csv_to_table(table_name, csv_file_path, connection, delimiter=',', header=True):
    for table in table_names:
        print("[PGSQL] loading", table)
        utilities.import_csv_to_table(table, "./data/"+table+".csv", connection, delimiter=',', header=header)

    # run analyze if allowed
    if RUN_ANALYZE:
        for table in table_names:
            print("[PGSQL] analyze table:", table)
            utilities.analyze_table(table, connection)

    #print(utilities.run_explain_analyze("SELECT * from h25_messages;", connection))
    #print(utilities.run_arbitrary("show jit;", connection))

    # run proposed optimization strategy
    if RUN_SOLUTION:
        for statement in solution:
            print("[PGSQL] running:", statement)
            utilities.run_optimisation(statement, connection)


    # get DB size
    dbsize = utilities.get_dbsize("dbUser", connection)
    print('[INFO] database size: ',dbsize)

    # run explain analyze
    overalcost = 0

    for i in range(WORKLOAD_RUNS):
        print("[INFO] RUN", i+1, "out of", WORKLOAD_RUNS)
        randomizeWorkload(queries)
        for q in queries:
            result= utilities.run_explain_analyze(q, connection)
            match = re.search(r'cost=\d+\.\d+\.\.(\d+\.\d+)', result[0][0])
            if match:
                cost = float(match.group(1))
                overalcost = overalcost + cost
            else:
                print("[Workload] error getting query cost")

    print('[INFO] overal cost: ', overalcost/float(WORKLOAD_RUNS))

    # we are done cleanup
    container.stop()
    container.remove(v=True)