
def loadWorkload():
    data = ""
    with open("workload/queries.txt") as f:
        for line in f:
            if line.startswith("--"):
                pass
            else:
                data += " " + line.strip()
    return data.split(";")

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