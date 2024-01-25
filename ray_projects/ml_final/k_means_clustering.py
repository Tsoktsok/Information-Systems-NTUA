import ray
import pandas as pd
from sklearn.cluster import KMeans
import time
import psutil
import numpy as np
import csv
import os

@ray.remote
def kmeans_clustering(start_line, end_line, model):
    HDFS_BASE_URL = "http://master:9870"
    file_path = args.file_path
    #file_path = "/user/user/ml_data/data18000000.csv"
    hdfs_client = InsecureClient(HDFS_BASE_URL, user='user')
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

    # Extract features and target variable
    X = [row[:-1] for row in rows]
    print(len(X))

    partition_predictions = model.predict(X)
    memory_usage = psutil.Process().memory_info().rss  # / 1e6  # Convert to megabytes

    return partition_predictions, memory_usage


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Generate a large dataset in CSV format.")
    parser.add_argument("--file_path", type=str, help="Path to the file")
    parser.add_argument("--dataset_path", type=str, help="Path to the dataset")
    parser.add_argument("--rows", type=int, help="Number of rows in the dataset", required=True)
    args = parser.parse_args()

    # Initialize Ray on all nodes
    ray.init(address='auto')


    # Load the first dataset
    dataset_path = args.dataset_path
    data = pd.read_csv(dataset_path)

    # Parallelize the model training across the Ray cluster
    X = data.drop('label', axis=1)
    y = data['label']

    # Initialize and train the Random Forest Regressor
    print("KSEKINAW PROPONHSH")
    num_clusters=10
    kmeans_model = KMeans(n_clusters=num_clusters)
    kmeans_model.fit(X)
    print("PROPONHSH TELOS")

    n = 500000
    num_tasks = int((args.rows/n)+1)

    # Track start time
    start_time = time.time()
    print("KSEKINAW DOULEIA")

    results_refs = [kmeans_clustering.remote(i*n, (i+1)*n-1, rf_model) for i in range(num_tasks)]
    # Combine results
    results = ray.get(results_refs)

    # Unpack the results into separate lists
    kmeans_results, kmeans_memory = zip(*results)

    # Track end time
    end_time = time.time()

    # Calculate training time
    predict_time = end_time - start_time

    # Combine results
    clusters = np.concatenate([result for result in kmeans_results])

    # Measure memory usage
    memory_usage = np.sum([memory for memory in kmeans_memory])

    num_rows = data.shape[0]
    num_features = len(data.columns)

    print(predict_time)
    print(memory_usage)
    print(num_rows)
    print(num_features)

    csv_file_path="/home/user/ray_projects/ml/ray_stats_ml.csv"
    try:
        with open(csv_file_path, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([predict_time, memory_usage, num_rows, num_features])
            print("Successfully wrote to CSV file.")
    except Exception as e:
        print(f"Error during file writing: {e}")

    # Shutdown Ray
    ray.shutdown()