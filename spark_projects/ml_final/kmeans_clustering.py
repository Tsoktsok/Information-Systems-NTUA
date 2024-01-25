from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel
import time
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from measure_performance import measure_performance as mp
from pyspark.ml.feature import VectorAssembler
from hdfs import InsecureClient
import csv
import argparse


parser = argparse.ArgumentParser(description="Generate a large dataset in CSV format.")
parser.add_argument("--process_data_path", type=str, help="Path to the file")
parser.add_argument("--train_data_path", type=str, help="Path to the dataset")
args = parser.parse_args()

# Create a Spark session
spark = SparkSession.builder.appName("ML_Random_Forest").getOrCreate()
sc = spark.sparkContext

# Specify the path to your CSV file
csv_file_path = args.train_data_path #"ml_data/data22_b.csv"

# Read the CSV file into a Spark DataFrame
dataset_draft = spark.read.csv(csv_file_path, header=True, inferSchema=True)

feature_columns = ["feature_" + str(i) for i in range(1, len(dataset_draft.columns))]

# Add the label column name as the last element in the list
#label_column = "label"
#feature_columns.append(label_column)

# Create a VectorAssembler to combine the feature columns into a vector
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")

# Apply the assembler to your DataFrame
dataset = assembler.transform(dataset_draft)

# Persist the DataFrame with MEMORY_AND_DISK storage level
#dataset.persist(StorageLevel.MEMORY_AND_DISK)

#define model
kmeans_model = KMeans(featuresCol="features", predictionCol="cluster", k=3)  # You can adjust the value of k
trained_model = kmeans_model.fit(dataset)


data_file_path = args.process_data_path #"ml_data/data18000000.csv"

start_time_load = time.time()
pred_data_draft = spark.read.csv(data_file_path, header=True, inferSchema=True)
num_rows = pred_data_draft.count()
print(num_rows)
end_time_load = time.time()
time_load = end_time_load - start_time_load

feature_columns = ["feature_" + str(i) for i in range(1, len(pred_data_draft.columns))]

# Add the label column name as the last element in the list
label_column = "label"
feature_columns.append(label_column)

# Create a VectorAssembler to combine the feature columns into a vector
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")

# Apply the assembler to your DataFrame
pred_data = assembler.transform(pred_data_draft)

# Persist the DataFrame with MEMORY_AND_DISK storage level
#pred_data.persist(StorageLevel.MEMORY_AND_DISK)


total_time = 0
time_process = 0

try:
    start_time_process = time.time()
    mp.measure_performance(trained_model, pred_data, "spark_stats_ml.csv")
    print("DONE")
    end_time_process = time.time()
    time_process = end_time_process - start_time_process
    print(total_time)

except Exception as e:
    print(e)

total_time = end_time_process - start_time_load
app_id = mp.get_app_id()
executors = mp.get_executor_summary(app_id)
memory_usage = mp.get_total_memory_usage(executors)

# Get the number of rows, features and memory size
num_rows_training = dataset.count()
num_rows_process = pred_data.count()

num_features = len(pred_data.columns) - 1

tool = "spark"
process = "clustering"
num_nodes = sc._jsc.sc().getExecutorMemoryStatus().keySet().size()

#predict_time, memory_usage, num_nodes,num_rows_training, num_rows_process, num_features, tool, process

csv_file_path= "spark_stats_ml.csv"
with open(csv_file_path, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([total_time, time_load , time_process ,memory_usage, num_nodes, num_rows_training, num_rows_process, num_features,tool, process])


#pred_data.unpersist()