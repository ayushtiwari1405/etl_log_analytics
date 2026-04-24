import psycopg2

def show_report(config, query):
    conn = psycopg2.connect(
        host=config["database"]["host"],
        port=config["database"]["port"],
        dbname=config["database"]["name"],
        user=config["database"]["user"],
        password=config["database"]["password"]
    )

    cur = conn.cursor()

    cur.execute(
        """
        SELECT run_id
        FROM etl_results
        WHERE query_name = %s
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (query,)
    )

    result = cur.fetchone()

    if not result:
        print("\n=== REPORT ===\n")
        print("No data found.")
        return

    latest_run_id = result[0]

    cur.execute(
        """
        SELECT run_id, key, value1, value2, value3, created_at
        FROM etl_results
        WHERE query_name = %s AND run_id = %s
        ORDER BY key
        """,
        (query, latest_run_id)
    )

    rows = cur.fetchall()

    print("\n=== REPORT ===\n")

    for row in rows:
        run_id, key, v1, v2, v3, ts = row
        print(f"[Run: {run_id}] {key} | {v1} | {v2} | {v3} | {ts}")

    cur.close()
    conn.close()