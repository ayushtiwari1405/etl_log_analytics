import subprocess
from pipelines.mapreduce.batch_runner import run_mapreduce_batch

def run_pipeline(pipeline, config, query):
    if pipeline == "mapreduce":
        run_mapreduce_batch(config, query)

    elif pipeline == "mongo":
        print("Mongo pipeline not implemented yet")

    elif pipeline == "hive":
        print("Hive pipeline not implemented yet")

    elif pipeline == "pig":
        print("Pig pipeline not implemented yet")

    else:
        print("Unknown pipeline")
