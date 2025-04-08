import os
from subprocess import PIPE, Popen
import platform
from time import sleep

import utilities
from random import shuffle as randomizeWorkload
import psycopg2
import docker

PG_PORT = 35432
PG_ADDRESS = "localhost"


if __name__ == '__main__':
    # load queries in memory
    queries = utilities.loadWorkload()
    print(queries)

    # Connect to the local docker daemon
    dd = docker.from_env()
    container = dd.containers.run("postgres:14", name="evalOptimBDD", ports={"5432":PG_PORT},
                                  environment=["POSTGRES_PASSWORD=mysecretpassword","POSTGRES_USER=dbUser"],
                                  detach=True)
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

    print(utilities.run_explain_analyze("SELECT version();", connection))

    # we are done cleanup
    container.stop()
    container.remove()