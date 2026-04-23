import psycopg2
import os

def load_results_to_db(config, query, run_id, file_path):
    conn = psycopg2.connect(
        host=config["database"]["host"],
        port=config["database"]["port"],
        dbname=config["database"]["name"],
        user=config["database"]["user"],
        password=config["database"]["password"]
    )

    cur = conn.cursor()

    with open(file_path, "r") as f:
        for line in f:
            parts = line.strip().split("\t")

            key = parts[0]
            values = parts[1:]

            v1 = float(values[0]) if len(values) > 0 else None
            v2 = float(values[1]) if len(values) > 1 else None
            v3 = float(values[2]) if len(values) > 2 else None

            cur.execute(
                """
                INSERT INTO etl_results (pipeline, query_name, run_id, batch_id, key, value1, value2, value3)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                ("mapreduce", query, run_id, 0, key, v1, v2, v3)
            )

    conn.commit()
    cur.close()
    conn.close()
