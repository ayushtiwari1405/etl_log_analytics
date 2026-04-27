import os
import time
import subprocess
from sql.load_results import load_results_to_db
from pipelines.pig.reducer import reduce_q1, reduce_q2, reduce_q3


def run_pig_batch(config, query, input_choice):

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

    for file_path in input_files:

        print(f"\n=== Processing File: {file_path} ===")

        with open(file_path, "r") as f:
            lines = f.readlines()

        total_records = len(lines)
        batches = [lines[i:i+batch_size] for i in range(0, total_records, batch_size)]

        run_id = str(int(time.time()))
        total_start = time.time()

        temp_q1, temp_q2, temp_q3 = [], [], []

        # -------------------------
        # BATCH EXECUTION
        # -------------------------
        for idx, batch in enumerate(batches, start=1):

            batch_file = f"results/logs/pig_batch_{run_id}_{idx}.log"
            output_dir = f"/tmp/pig_out_{run_id}_{idx}"

            with open(batch_file, "w") as bf:
                bf.writelines(batch)

            if os.path.exists(output_dir):
                os.system(f"rm -rf {output_dir}")

            log_file = f"results/logs/pig_exec_{run_id}_{idx}.log"

            # ---------- ETL ----------
            with open(log_file, "w") as log:
                subprocess.run(
                    [
                        "pig", "-x", "local",
                        "-param", f"INPUT={batch_file}",
                        "-param", f"OUTPUT={output_dir}",
                        "pipelines/pig/etl.pig"
                    ],
                    stdout=log,
                    stderr=log,
                    check=True
                )

            # ---------- QUERIES ----------
            with open(log_file, "a") as log:
                subprocess.run(
                    [
                        "pig", "-x", "local",
                        "-param", f"INPUT={output_dir}",
                        "-param", f"OUTPUT={output_dir}",
                        "pipelines/pig/queries.pig"
                    ],
                    stdout=log,
                    stderr=log,
                    check=True
                )
            # ---------- MALFORMED COUNT ----------
            malformed_file = f"{output_dir}/malformed/part-m-00000"

            if os.path.exists(malformed_file):
                with open(malformed_file) as mf:
                    malformed_count = len(mf.readlines())
                    print(f"Malformed Count: {malformed_count}")
            # -------------------------
            # COLLECT OUTPUTS
            # -------------------------
            q1_file = f"results/logs/pig_q1_{run_id}_{idx}.txt"
            q2_file = f"results/logs/pig_q2_{run_id}_{idx}.txt"
            q3_file = f"results/logs/pig_q3_{run_id}_{idx}.txt"

            os.system(f"cat {output_dir}/q1/* > {q1_file}")
            os.system(f"cat {output_dir}/q2/* > {q2_file}")
            os.system(f"cat {output_dir}/q3/* > {q3_file}")

            # ---------- KEEP ONLY REQUIRED ----------
            if query == "q1":
                if os.path.exists(q2_file): os.remove(q2_file)
                if os.path.exists(q3_file): os.remove(q3_file)
                temp_q1.append(q1_file)

            elif query == "q2":
                if os.path.exists(q1_file): os.remove(q1_file)
                if os.path.exists(q3_file): os.remove(q3_file)
                temp_q2.append(q2_file)

            elif query == "q3":
                if os.path.exists(q1_file): os.remove(q1_file)
                if os.path.exists(q2_file): os.remove(q2_file)
                temp_q3.append(q3_file)

        # -------------------------
        # MERGE + REDUCE
        # -------------------------
        if query == "q1":
            merged_q1 = f"results/logs/pig_merged_q1_{run_id}.txt"
            with open(merged_q1, "w") as fout:
                for f in temp_q1:
                    fout.writelines(open(f).readlines())

            final_q1 = f"results/outputs/pig_q1_{run_id}.txt"
            reduce_q1(merged_q1, final_q1)
            load_results_to_db(config, "q1", run_id, final_q1)

        elif query == "q2":
            merged_q2 = f"results/logs/pig_merged_q2_{run_id}.txt"
            with open(merged_q2, "w") as fout:
                for f in temp_q2:
                    fout.writelines(open(f).readlines())

            final_q2 = f"results/outputs/pig_q2_{run_id}.txt"
            reduce_q2(merged_q2, final_q2)
            load_results_to_db(config, "q2", run_id, final_q2)

        elif query == "q3":
            merged_q3 = f"results/logs/pig_merged_q3_{run_id}.txt"
            with open(merged_q3, "w") as fout:
                for f in temp_q3:
                    fout.writelines(open(f).readlines())

            final_q3 = f"results/outputs/pig_q3_{run_id}.txt"
            reduce_q3(merged_q3, final_q3)
            load_results_to_db(config, "q3", run_id, final_q3)

        total_end = time.time()

        return {
            "pipeline": "pig",
            "file": file_path,
            "records": total_records,
            "batches": len(batches),
            "runtime": total_end - total_start
        }