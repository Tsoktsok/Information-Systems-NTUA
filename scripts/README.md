## Script Instructions Guide

To use the scripts and compare the two frameworks, follow these steps:

# 1. Download the Code:

Clone the entire repository to your local machine:
```
git clone https://github.com/your-username/Information-Systems-NTUA
```
Ensure that you copy all code and files within the provided directory structure without changing the directory layout.
Also make sure you have run the install_dependencies.sh as mentioned in the projects README file.

# 2. Set Up HDFS and Ray Clusters:
Execute the following commands to set up HDFS (Hadoop Distributed File System):
```
start-dfs.sh
start-yarn.sh
```

For Ray, run the following commands:
  On the master node (with no IP connected):
  ```
  ray start --head
  ```
  Other nodes:
  ```
  ray start --address=<master address>
  ```
Now connect the IP to the master node.

For steps 3 and 4 do:
```
cd scripts
```

# 3. Run the graph script
Before running the graph tests, make sure to set the execute permission for the "graphs.sh" script:
```
chmod +x graphs.sh
```
Run the graph tests using the following command:
```
./graphs.sh
```
To change the number of graph nodes and the probability of connections, modify the num_nodes and possibility variables in the "graphs.sh" script.

# 4 Run the Machine Learning Tests:

Before running the machine learning tests, make sure to set the execute permission for the scripts:
```
chmod +x ml_data_gen.sh ml_ray.sh ml_spark.sh delete_files.sh
```
To generate the datasets, run the following command:
```
./ml_data_gen.sh
```
The number of rows and features for each dataset  can be changed directly in the "ml_data_gen.sh" script.

To process the datasets using Ray, run:
```
./ml_ray.sh
```

To process the datasets using Spark, run:
```
./ml_spark.sh
```

Finally, to delete the big-sized datasets and clear up space, run:
```
./delete_files.sh
```






  

