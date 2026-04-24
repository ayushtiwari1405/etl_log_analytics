import argparse
import yaml
import os
from dotenv import load_dotenv
from controller.pipeline_selector import run_pipeline
from reporting.report import show_report


def load_config():
    with open("common/config.yaml", "r") as f:
        return yaml.safe_load(f)


def main():
    load_dotenv()

    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline", required=True, help="mongo | hive | pig | mapreduce")
    parser.add_argument("--query", required=True, help="q1 | q2 | q3")
    parser.add_argument("--batch_size", type=int, help="override batch size")
    parser.add_argument("--report", action="store_true")
    parser.add_argument("--input", default="sample")

    args = parser.parse_args()

    config = load_config()

    if args.pipeline not in config["pipelines"]["available"]:
        print("Invalid pipeline selected")
        return

    if args.batch_size:
        config["batch"]["size"] = args.batch_size

    db_password = os.getenv("DB_PASSWORD")
    config["database"]["password"] = db_password

    result = run_pipeline(args.pipeline, config, args.query, args.input)

    if result:
        print("\n--- FILE SUMMARY ---")
        print(f"Pipeline: {result.get('pipeline')}")
        print(f"File: {result.get('file')}")
        print(f"Records: {result.get('records')}")
        print(f"Batches: {result.get('batches')}")
        print(f"Runtime: {result.get('runtime'):.4f}s")

    if args.report:
        show_report(config, args.query)


if __name__ == "__main__":
    main()