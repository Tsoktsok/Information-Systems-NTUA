from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel
from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressor
from measure_performance import measure_performance as mp
from pyspark.ml.feature import VectorAssembler
from hdfs import InsecureClient

# Create a Spark session
spark = SparkSession.builder.appName("ML_Random_Forest").getOrCreate()

# Specify the path to your CSV file
csv_file_path = "ml_data/data13.csv"

# Read the CSV file into a Spark DataFrame
dataset_draft = spark.read.csv(csv_file_path, header=True, inferSchema=True)

feature_columns = ["feature_" + str(i) for i in range(1, len(dataset_draft.columns))]

# Add the label column name as the last element in the list
label_column = "label"
feature_columns.append(label_column)

# Create a VectorAssembler to combine the feature columns into a vector
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")

# Apply the assembler to your DataFrame
dataset = assembler.transform(dataset_draft)

# Persist the DataFrame with MEMORY_AND_DISK storage level
dataset.persist(StorageLevel.MEMORY_AND_DISK)

print("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
#define model
random_forest_model = RandomForestRegressor()

try:
    mp.measure_performance(random_forest_model, dataset, "stats_ml.csv")
    print("DONE")

except Exception as e:
    print(e)

dataset.unpersist()
