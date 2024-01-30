#!/bin/bash


# Generate a random integer between 1 and 3000
num_nodes=$((RANDOM % 1000 + 1001))

possibility=0.75

./graph_gen/graph_generator $num_nodes $possibility "graph_1.csv"

echo " graph ready"
hdfs dfs -copyFromLocal "graph_1.csv" "/user/user/graph_data"
echo "graph in hdfs"
spark-submit graphs-assembly-1.0.jar "pageRank" "hdfs://master:54310/user/user/graph_data/graph_1.csv"
echo "spark done"
RAY_ADDRESS='http://127.0.0.1:8265' ray job submit -- python3 /home/user/ray_projects/graphs/pagerank.py "page_rank" "/user/user/graph_data/graph_1.csv"  
echo "ray done"
hdfs dfs -rm "/user/user/graph_data/graph_1.csv"
echo "graph removed"





