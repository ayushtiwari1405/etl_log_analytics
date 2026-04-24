import os
import time
import subprocess
from sql.load_results import load_results_to_db

def run_mapreduce_batch(config, query, input_choice):

    if input_choice == "sample":
        input_files = ["data/sample/sample.log"]

    elif input_choice == "jul":
        input_files = ["data/raw/NASA_access_log_Jul95"]

    elif input_choice == "aug":
        input_files = ["data/raw/NASA_access_log_Aug95"]

    elif input_choice == "both":
        input_files = [
            "data/raw/NASA_access_log_Jul95",
            "data/raw/NASA_access_log_Aug95"
        ]

    batch_size = config["batch"]["size"]

    os.makedirs("results/logs", exist_ok=True)
    os.makedirs("results/outputs", exist_ok=True)

    last_summary = None 

    for file_path in input_files:

        print(f"\n=== Processing File: {file_path} ===")

        with open(file_path, "r") as f:
            lines = f.readlines()

        total_records = len(lines)
        batches = [lines[i:i+batch_size] for i in range(0, total_records, batch_size)]

        run_id = str(int(time.time()))
        total_start = time.time()

        temp_outputs = []

        # -------------------------
        # BATCH EXECUTION
        # -------------------------
        for idx, batch in enumerate(batches, start=1):
            batch_file = f"results/logs/batch_{run_id}_{idx}.log"
            output_file = f"results/logs/out_{run_id}_{idx}.txt"

            with open(batch_file, "w") as bf:
                bf.writelines(batch)

            subprocess.run(
                ["bash", "pipelines/mapreduce/driver.sh", query, batch_file, output_file],
                check=True
            )

            temp_outputs.append(output_file)

        # -------------------------
        # MERGE OUTPUTS
        # -------------------------
        merged_file = f"results/logs/merged_{run_id}.txt"
        final_output = f"results/outputs/mr_{query}_{run_id}.txt"

        with open(merged_file, "w") as fout:
            for temp_file in temp_outputs:
                with open(temp_file, "r") as fin:
                    fout.writelines(fin.readlines())

        # -------------------------
        # FINAL REDUCE
        # -------------------------
        base = f"pipelines/mapreduce/{query}"
        reducer_exec = f"{base}/reducer"

        if query == "q1":
            subprocess.run(
                ["g++", f"{base}/reducer.cpp", "-o", reducer_exec],
                check=True
            )

            with open(final_output, "w") as fout:
                subprocess.run(
                    f"cat {merged_file} | sort | {reducer_exec}",
                    shell=True,
                    stdout=fout,
                    check=True
                )
        else:
            with open(final_output, "w") as fout:
                with open(merged_file, "r") as fin:
                    fout.writelines(fin.readlines())

        # -------------------------
        # LOAD INTO DATABASE
        # -------------------------
        load_results_to_db(config, query, run_id, final_output)

        total_end = time.time()

        # -------------------------
        # STORE SUMMARY (DO NOT PRINT HERE)
        # -------------------------
        last_summary = {
            "pipeline": "mapreduce",
            "file": file_path,
            "records": total_records,
            "batches": len(batches),
            "runtime": total_end - total_start
        }

    return last_summary 