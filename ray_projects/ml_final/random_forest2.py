import ray
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
import time
import psutil
import numpy as np
import csv
import os
from hdfs import InsecureClient
from itertools import islice
import argparse

# Function to train a Random Forest Regressor model and measure time and memory usage
@ray.remote
def rf_prediction(start_line, end_line, rf_model):
    HDFS_BASE_URL = "http://master:9870"
    file_path = args.process_data_path
    #file_path = "/user/user/ml_data/data18000000.csv"
    hdfs_client = InsecureClient(HDFS_BASE_URL, user='user')
    #start_time = time.time()
    with hdfs_client.read(file_path, encoding='utf-8') as file:
        # Skip lines until you reach the starting line
        for _ in range(start_line):
            next(file)
        # Read the desired lines within the specified range
        rows = []
        for line in islice(file, end_line - start_line + 1):
            try:
                row = list(map(int, line.strip().split(',')))
                rows.append(row)
            except ValueError:
                # Skip lines that cannot be converted to integers
                pass
    #load_time = time.time() - start_time
    #data = pd.read_csv(dataset_path)
    #data_id = ray.put(data)

    # Extract features and target variable
    X = [row[:-1] for row in rows]
    print(len(X))

    #start_time = time.time()
    partition_predictions = rf_model.predict(X)
    #end_time = time.time()
    #predict_time = end_time - start_time
    memory_usage = psutil.Process().memory_info().rss  # / 1e6  # Convert to megabytes

    return partition_predictions, memory_usage

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a large dataset in CSV format.")
    parser.add_argument("--process_data_path", type=str, help="Path to the file")
    parser.add_argument("--train_data_path", type=str, help="Path to the dataset")
    parser.add_argument("--rows", type=int, help="Number of rows in the dataset", required=True)
    args = parser.parse_args()

    # Initialize Ray on all nodes
    ray.init(address='auto')


    # Load the first dataset
    dataset_path = args.train_data_path
    data = pd.read_csv(dataset_path)

    # Parallelize the model training across the Ray cluster
    X = data.drop('label', axis=1)
    y = data['label']

    # Initialize and train the Random Forest Regressor
    print("KSEKINAW PROPONHSH")
    rf_model = RandomForestRegressor()
    rf_model.fit(X, y)
    print("TELOS PROPONHSH")
    n = 500000
    num_tasks = int((args.rows/n)+1)

    # Track start time
    start_time = time.time()

    # Pass the data_id to the remote function instead of the data partition
    print("ORA GIA MANTEPSIES")
    results_refs = [rf_prediction.remote(i*n, (i+1)*n-1, rf_model) for i in range(num_tasks)]

    # Combine results
    results = ray.get(results_refs)

    # Unpack the results into separate lists
    rf_results, rf_memory = zip(*results)

    # Track end time
    end_time = time.time()

    # Calculate training time
    total_time = end_time - start_time

    # Combine results
    predictions = np.concatenate([result for result in rf_results])

    #first_3_rows_predictions = [result[:3] for result in rf_results]

    # Print the first 3 rows of predictions
   # print("First 3 rows of predictions from each partition:")
    #for i, partition_predictions in enumerate(first_3_rows_predictions):
     #   print(f"Partition {i + 1}: {partition_predictions}")

    # Measure memory usage
    memory_usage = np.sum([memory for memory in rf_memory])

    num_rows = data.shape[0]
    num_features = len(data.columns)

    print(total_time)
    print(memory_usage)
    print(args.rows)
    print(num_features)

    nodes = ray.nodes()

    csv_file_path = "/home/user/ray_projects/ml/ray_stats_ml.csv"
    try:
        with open(csv_file_path, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([total_time, predict_time, load_time, memory_usage, len(nodes), num_rows, args.rows, num_features, "ray", "prediction"])
            print("Successfully wrote to CSV file.")
    except Exception as e:
        print(f"Error during file writing: {e}")

    # Shutdown Ray
    ray.shutdown()