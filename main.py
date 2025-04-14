import os
from email import header
from subprocess import PIPE, Popen
import platform
from time import sleep, time, localtime, strftime
import re
import pandas as pd

import utilities
from random import shuffle as randomizeWorkload
import psycopg2
import docker

from Config import Config
from utilities import run_explain_analyze, run_optimisation
USER="PM"

#PG_VERSION = "14"
#PG_PORT = 35432
#PG_PORT = 5432
#PG_ADDRESS = "localhost"

RUN_ANALYZE = False
RUN_SOLUTION = True
WORKLOAD_RUNS = 1
VALIDATE_STATEMENTS = False
NO_OPTIM_RUN = True
ZIPPED = True
DOCKER = False
CLEANUP = False
EVALUATE = True
IMPORT_STUDENT = False
CREATE_STUDENT = False


def create_table(connection, solution_partition, tables, student_table_names):
    # create tables
    #       student's
    for q in solution_partition:
        utilities.run_create(q, connection)
    #       default
    for i in range(len(tables)):
        query = tables[i]
        if table_names[i] not in student_table_names:
            res = utilities.run_create(query, connection)
    print("[PGSQL] tables created")

def import_data(connection,table_names):
    # import data
    # import_csv_to_table(table_name, csv_file_path, connection, delimiter=',', header=True):
    for table in table_names:
        print("[PGSQL] loading", table)
        #utilities.import_csv_to_table(table, "./data/" + table + ".csv", connection, delimiter=',', header=header)
        utilities.import_csv_to_table_alt(table, config.path_to_data + table + ".csv", connection, delimiter=',', header=header)

def run_analyze(connection,table_names):
        for table in table_names:
            print("[PGSQL] analyze table:", table)
            utilities.analyze_table(table, connection)

    #print(utilities.run_explain_analyze("SELECT * from h25_messages;", connection))
    #print(utilities.run_arbitrary("show jit;", connection))

def run_optimisations(connection,solution):
        for statement in solution:
            print("[PGSQL] running:", statement)
            utilities.run_optimisation(statement, connection)

def compute_cost(connection,WORKLOAD_RUNS,queries):
    overalcost = 0

    for i in range(WORKLOAD_RUNS):
        print("[INFO] RUN", i + 1, "out of", WORKLOAD_RUNS)
        randomizeWorkload(queries)
        for q in queries:
            result = utilities.run_explain_analyze(q, connection)
            match = re.search(r'cost=\d+\.\d+\.\.(\d+\.\d+)', result[0][0])
            if match:
                cost = float(match.group(1))
                overalcost = overalcost + cost
            else:
                print("[Workload] error getting query cost")

    print('[INFO] overal cost: ', overalcost / float(WORKLOAD_RUNS))
    return overalcost / float(WORKLOAD_RUNS)

def cleanup(connection, DOCKER):
    if DOCKER:
        container.stop()
        container.remove(v=True)
    else:
        utilities.dropDB(connection, Config.dbname)

if __name__ == '__main__':
    current_time = localtime()
    formatted_time = strftime("%d-%m-%y:%H:%M:%S", current_time)
    fileResults = 'results/results_' + formatted_time +  '.csv'
    column_names = ['name', 'db size','cost']

    # Create an empty DataFrame with the specified columns
    dfres = pd.DataFrame(columns=column_names)

    config = Config('configs/postgres.ini', USER)

    # run local docker container
    if DOCKER:
        dd = docker.from_env()
        container = dd.containers.run("postgres:" + Config.version, "-c random_page_cost=1.4 -c jit=off",
                                      name="evalOptimBDD", ports={"5432": Config.port},
                                      environment=["POSTGRES_PASSWORD=mysecretpassword", "POSTGRES_USER=dbUser"],
                                      detach=True)  # volumes=['tp_bdd_optim_data:/to_import']
        sleep(2)
        print("[DOCKER] Postgres instance is up and running")

    # establish database connection
    connection_params = {
        'dbname': 'hackathon',
        'user': '',
        'password': '',
        'host': config.host,
        'port': config.port
    }
    # connect
    connection_ok = False
    while not connection_ok:
        try:
            connection = psycopg2.connect(**connection_params)
            connection_ok = True
        except Exception as e:
            print(f"Error connecting to PostgreSQL: {e}")
            sleep(5)



    # load queries in memory
    queries = utilities.loadWorkload(config.workload)
    tables = utilities.loadWorkload(config.schema)
    table_names = [utilities.extract_table_name(t).lstrip("public.") for t in tables]
    print('[INFO] ',len(tables), 'tables to create :', table_names)
    print('[INFO] ',len(queries), 'queries to run.')




    # computes size and cost for no optimization
    if NO_OPTIM_RUN:
        create_table(connection, tables, [], [])
        import_data(connection,table_names)
        dbsize_nooptim = utilities.get_dbsize(config.dbname, connection)
        print('[INFO] database size without optimization: ', dbsize_nooptim)
        cost = compute_cost(connection, WORKLOAD_RUNS, queries)
        print('[INFO] this cost is without optimization: ')
        dfres.loc[len(dfres)] = ["no optimisation", dbsize_nooptim, cost]

    data=[]
    # Extracting students answers
    if ZIPPED:
        try:
            subfolder_path = utilities.unzip_and_get_subfolder(config.path_to_zip)
            print(f"[UNZIP] Extracted subfolder path: {subfolder_path}")
            data = utilities.explore_folder(subfolder_path)
            print(f"[INFO] Number of answers: {len(data)}")
            ZIPPED = False
        except Exception as e:
            print(f"Error in extracting from zip: {e}")

    #print(data)
    # evaluation
    if EVALUATE:
        for subfolder_path,prefix, file1, file2 in data:
            print(f"[INFO] exploring directory: {subfolder_path}")
            print(f"[INFO] for student: {prefix}")
            #print(f"[INFO] create file : {file1}")
            #print(f"[INFO] workload file: {file2}")
            #    print("------")
            #solution = utilities.split_sql_statements(open("workload/student_setup.txt").read())
            #solution_partition = utilities.loadWorkload("workload/student_create.txt")
            solution = utilities.split_sql_statements(open(subfolder_path+"/"+config.student_setup).read())
            solution_partition = utilities.loadWorkload(subfolder_path+"/"+config.student_create)
            student_table_names = [utilities.extract_table_name(t).lstrip("public.") for t in solution_partition]

            if VALIDATE_STATEMENTS:
                for st in solution:
                    if utilities.is_valid_postgres_sql(st):
                        pass
                    else:
                        print("INVALID SQL ? :", st)
                        exit(0)

            # creating tables for the current solution
            if CREATE_STUDENT:
                create_table(connection, solution_partition, tables, student_table_names)

            # import data for the current solution
            if IMPORT_STUDENT:
                import_data(connection,student_table_names)

            # run analyze for the current solution
            if RUN_ANALYZE:
                run_analyze(connection, table_names)

            # run proposed optimization strategy
            if RUN_SOLUTION:
                run_optimisations(connection, solution)

            # get DB size
            dbsize = utilities.get_dbsize(config.dbname, connection)
            print('[INFO] database size: ',dbsize)

            # run explain analyze
            cost=compute_cost(connection,WORKLOAD_RUNS,queries)

            dfres.loc[len(dfres)] = [prefix, dbsize, cost]

            #reset for next student
            utilities.dropAllTables(connection)
            create_table(connection, tables, [], [])
            import_data(connection, table_names)
            print('[INFO] reset done')

        dfres.to_csv(fileResults, mode='a', header=False)

        # we are done cleanup
        if CLEANUP:
            cleanup(connection,DOCKER)

