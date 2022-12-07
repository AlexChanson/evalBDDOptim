import os
from subprocess import PIPE, Popen


pg_path = "/usr/bin/"
env_vars = dict(os.environ)
env_vars['PGPASSFILE'] = './.pgpass'

# exec student optimizations


# Analyse
with Popen([pg_path + "psql", "-h", "localhost", "-U", "marcel", "-d", 'teaching', '-f', "./workload/analyse.txt"], env=env_vars, stdout=PIPE, stderr=PIPE) as process:
    output = process.communicate()[0].decode("utf-8")
    print(output)

with Popen([pg_path + "psql", "-h", "localhost", "-U", "marcel", "-d", 'teaching', '-f', './workload/queries.txt'],
           env=env_vars, stdout=PIPE, stderr=PIPE) as process:
    output = process.communicate()[0].decode("utf-8")
    cost = 0
    j = 1
    fetch = False
    for line in output.splitlines():
        if line.startswith('-----'):
            fetch = True
            continue
        if fetch:
            fetch = False
            t = line.split('..')
            print('query ', j, ' : ', t[1].split()[0])
            j = j + 1
            cost = cost + float(t[1].split()[0])

print('overal cost: ', cost)

# computing size
with Popen([pg_path + "psql", "-h", "localhost", "-U", "marcel", "-d", 'teaching', '-c', "SELECT pg_size_pretty( pg_database_size('teaching') );"], env=env_vars, stdout=PIPE, stderr=PIPE) as process:
    output = process.communicate()[0].decode("utf-8")
    print("Size:", output.splitlines()[2])

# cleaning up
# os.system("/Applications/Postgres.app/Contents/Versions/11/bin/psql -d 'health insurance' -f /Users/marcel/Documents/ENSEIGNEMENTS/BD/DATASETS/health_insurance/health_insurance.sql")
