import gc
import time
import psutil
import csv
import requests
import json
import os
from pyspark.storagelevel import StorageLevel
from sparkmeasure import StageMetrics

# Get the Spark application ID
def get_app_id():
    response = requests.get("http://83.212.72.93:4040/api/v1/applications")
    applications = json.loads(response.text)
    app_id = applications[0]["id"]
    return app_id

# Get the executor summary
def get_executor_summary(app_id):
    response = requests.get(f"http://83.212.72.93:4040/api/v1/applications/{app_id}/executors")
    executors = json.loads(response.text)
    return executors

# Calculate the average CPU usage
def get_average_cpu_usage(executors):
    total_cpu = 0
    for executor in executors:
        print
        total_cpu += executor["totalCores"]
    average_cpu = total_cpu / len(executors)
    return average_cpu

# Calculate the total memory usage
def get_total_memory_usage(executors):
    total_memory = 0
    for executor in executors:
        total_memory += executor["memoryUsed"]
    return total_memory

# get the total time
def get_total_time(executors):
    total_time = 0
    for executor in executors:
        total_time += executor["totalDuration"]
    return total_time


def measure_performance(model, dataset, csv_file_path):
    # Start a process to measure the CPU and memory usage
    process = psutil.Process()

    start_time = time.time()

    # Fit the model to the dataset
    predictions = model.transform(dataset)
    #print("HERE ARE THE PREDS")
    #predictions.show()
    #num_rows = predictions.count()
    #print("HERE ARE THE ROWS")
    #print(num_rows)
    end_time = time.time()
    total_time = end_time - start_time

    app_id = get_app_id()
    executors = get_executor_summary(app_id)

    # Get the total memory from all the executors
    memory_usage = get_total_memory_usage(executors)

    print(f"Total Time: {total_time} seconds")
    print(f"Application ID: {app_id}")
    print("Executor Summary:")
    for executor in executors:
        print(f"  - Executor ID: {executor['id']}")
        print(f"    Cores: {executor['totalCores']}")
        print(f"    Memory Used: {executor['memoryUsed']} bytes")

    print(f"Total Memory Usage: {memory_usage} bytes")


