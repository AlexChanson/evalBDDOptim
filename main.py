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

PG_PORT = 35432
PG_ADDRESS = "localhost"
RUN_ANALYZE = True
RUN_SOLUTION = True


if __name__ == '__main__':
    # load queries in memory
    queries = utilities.loadWorkload("workload/queries.txt")
    tables = utilities.loadWorkload("workload/create.txt")
    solution = utilities.loadWorkload("workload/student_setup.txt")
    table_names = [utilities.extract_table_name(t).lstrip("public.") for t in tables]
    print(len(tables), 'tables to create :', table_names)
    print(len(queries), 'queries to run.')

    # Connect to the local docker daemon
    dd = docker.from_env()
    container = dd.containers.run("postgres:14", "-c random_page_cost=1.4 -c jit=off",name="evalOptimBDD", ports={"5432":PG_PORT},
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
    for query in tables:
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
    for q in queries:
        result= utilities.run_explain_analyze(q, connection)
        match = re.search(r'cost=\d+\.\d+\.\.(\d+\.\d+)', result[0][0])
        if match:
            cost = float(match.group(1))
            overalcost = overalcost + cost
        else:
            print("[Workload] error getting query cost")

    print('[INFO] overal cost: ',overalcost)

    # we are done cleanup
    container.stop()
    container.remove(v=True)