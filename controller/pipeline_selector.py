import subprocess
from pipelines.mapreduce.batch_runner import run_mapreduce_batch
from pipelines.pig.batch_runner import run_pig_batch


def run_pipeline(pipeline, config, query, input_choice):
    
    if pipeline == "mapreduce":
        return run_mapreduce_batch(config, query, input_choice)

    elif pipeline == "pig":
        print("Pig pipeline not implemented yet.")

    elif pipeline == "mongo":
        print("Mongo pipeline not implemented yet")

    elif pipeline == "hive":
        print("Hive pipeline not implemented yet")

    else:
        print("Unknown pipeline")
