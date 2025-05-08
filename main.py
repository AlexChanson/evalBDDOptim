import sys
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

INIT = True                     # to drop all tables
RUN_ANALYZE = True              # to run analyze if not included in the proposed solution
RUN_SOLUTION = True             # to run the proposed optimizations
WORKLOAD_RUNS = 1               # number of runs of the workload
VALIDATE_STATEMENTS = False     # to check if proposed statements are syntactically correct
NO_OPTIM_RUN = True             # to compute cost and size without optimization
ZIPPED = True                   # if proposed solutions are zipped
DOCKER = False                  # if postgres is used containerized
CLEANUP = False                 # to remove container (if containerized) or drop DB (otherwise)
EVALUATE = True                 # to evaluate the proposed solution
RECREATE = False                # to reset (recreate table and import) after each evaluation
CREATE_STUDENT = True           # to drop all tables and run the create tables of the proposed solution
IMPORT_STUDENT = True           # to import data after the proposed create tables
JUST_RESET = False              # to drop tables and recreate with import, then exit
EXPLAIN_ANALYZE = False         # if we want to execute the queries


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
            res=utilities.run_optimisation(statement, connection)
        #return res


def compute_cost(connection,WORKLOAD_RUNS,queries,EXPLAIN_ANALYZE = False):
    overalcost = 0

    for i in range(WORKLOAD_RUNS):
        print("[INFO] RUN", i + 1, "out of", WORKLOAD_RUNS)
        #randomizeWorkload(queries)
        for q in queries:
            if EXPLAIN_ANALYZE:
                result = utilities.run_explain_analyze(q, connection)
            else:
                result = utilities.run_explain(q, connection)
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
    column_names = ['name', 'db size','cost', 'size increase', 'cost decrease','score']

    # Create an empty DataFrame with the specified columns
    dfres = pd.DataFrame(columns=column_names)
    dfres.to_csv(fileResults)

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

    # to process every statement independently
    connection.autocommit = True
    print("[PGSQL] Autocommit is set to:", connection.autocommit)
    # max sample size for analyze
    utilities.default_stat(connection)

    # load queries in memory
    queries = utilities.loadWorkload(config.workload)
    tables = utilities.loadWorkload(config.schema)
    table_names = [utilities.extract_table_name(t).lstrip("public.") for t in tables]
    print('[INFO] ',len(tables), 'tables to create :', table_names)
    print('[INFO] ',len(queries), 'queries to run.')

    if JUST_RESET:
        utilities.dropAllTables(connection)
        create_table(connection, tables, [], [])
        import_data(connection, table_names)
        print('[INFO] reset done')
        sys.exit()

    if INIT:
        utilities.dropAllTables(connection)

    # computes size and cost for no optimization
    if NO_OPTIM_RUN:
        print("no optimisation run")
        create_table(connection, tables, [], [])
        import_data(connection,table_names)
        run_analyze(connection, table_names)
        dbsize_nooptim = utilities.get_dbsize(config.dbname, connection)
        dbsize_nooptim = int(dbsize_nooptim.split(' ')[0])
        print('[INFO] database size without optimization: ', dbsize_nooptim)
        cost_nooptim = compute_cost(connection, WORKLOAD_RUNS, queries,EXPLAIN_ANALYZE)
        print('[INFO] this cost is without optimization: ')
        dfres.loc[len(dfres)] = ["no optimisation", dbsize_nooptim, cost_nooptim, 1, 1,1]

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

    list_problems = []
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
            #student_table_names = [utilities.extract_table_name(t).lstrip("public.") for t in solution_partition]
            student_table_names = []
            for  t in solution_partition:
                if utilities.extract_table_name(t) is not None:
                    student_table_names.append(utilities.extract_table_name(t).lstrip("public."))

            problem_detected = False
            if VALIDATE_STATEMENTS:
                for st in solution:
                    if utilities.is_valid_postgres_sql(st):
                        pass
                    else:
                        print("[INVALID SQL] for student: ", prefix)
                        print("[INVALID SQL] :", st)
                        print('[INVALID SQL] Moving on to the next student')
                        problem_detected = True
                        break
                        #exit(0)

            if not problem_detected:
                # creating tables for the current solution
                if CREATE_STUDENT:
                    utilities.dropAllTables(connection)
                    utilities.vacuum(connection)
                    create_table(connection, solution_partition, tables, student_table_names)

                # import data for the current solution
                if IMPORT_STUDENT:
                    import_data(connection,table_names)

                # run analyze for the current solution
                if RUN_ANALYZE:
                    run_analyze(connection, table_names)

                # run proposed optimization strategy
                if RUN_SOLUTION:
                    problem_detected = False
                    try:
                        res=run_optimisations(connection, solution)
                    except Exception as e:
                        print(f"Error running student optimization: {e}")
                        list_problems.append(prefix)
                        #print('Moving on to the next student')
                        #utilities.dropAllTables(connection)
                        #problem_detected = True


                if not problem_detected:
                    # get DB size
                    dbsize = utilities.get_dbsize(config.dbname, connection)
                    dbsize = int(dbsize.split(' ')[0])
                    print('[INFO] database size: ',dbsize)

                    # run explain analyze
                    cost=compute_cost(connection,WORKLOAD_RUNS,queries,EXPLAIN_ANALYZE)
                    sizeinc = 1 + ((dbsize - dbsize_nooptim) / dbsize_nooptim)
                    costdec =  1 + ((cost_nooptim - cost) / cost_nooptim)
                    score =  costdec / sizeinc
                    dfres.loc[len(dfres)] = [prefix, dbsize, cost, sizeinc, costdec, score]
                    dfres.to_csv(fileResults, mode='w', header=False)
                    print('[INFO] score of ',prefix,' is: ',score)

                    #reset for next student
                    if RECREATE:
                        utilities.dropAllTables(connection)
                        create_table(connection, tables, [], [])
                        import_data(connection, table_names)
                        print('[INFO] reset done')

        dfres.to_csv(fileResults, mode='w', header=False)

        print('[INFO] Solutions with problems: ',list_problems)
        # we are done cleanup
        if CLEANUP:
            cleanup(connection,DOCKER)