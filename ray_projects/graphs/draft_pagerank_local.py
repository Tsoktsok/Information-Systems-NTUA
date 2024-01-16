import ray
import time
import psutil
import networkx as nx
from hdfs import InsecureClient

ray.init()

HDFS_BASE_URL = "http://master:9870"

@ray.remote
def load_graph(file_path):
    # Connect to HDFS

    
    start_time = time.time()
    # Read file from HDFS
    edges = [tuple(map(int, line.split()))
             for line in open(file_path).readlines()]

    elapsed_time = time.time() - start_time
    memory_info = psutil.virtual_memory()

    return edges, elapsed_time, memory_info


@ray.remote
def page_rank(edges):
   
    # Create a directed graph
    graph = nx.DiGraph(edges)

    # Perform PageRank algorithm using networkx
    start_time = time.time()
    result = nx.pagerank(graph)
    elapsed_time = time.time() - start_time

    num_vertices = graph.number_of_nodes()
    num_edges = graph.number_of_edges()

    return num_vertices, num_edges, elapsed_time, psutil.virtual_memory()


def main(algorithm_to_execute, dataset_size):
    if algorithm_to_execute not in ["page_rank", "connected_components", "triangle_counting"]:
        print("Invalid algorithm name. Supported algorithms: page_rank, connected_components, triangle_counting")
        return
    
    edges, load_time, load_memory_info = ray.get(load_graph.remote("/../home/user/graph_gen/graph_13000_075.txt"))

    # Create a single remote task for PageRank
    process_task = eval(algorithm_to_execute).remote(edges)

    # Get result from the remote task
    ray_graph_info = ray.get(process_task)

    num_vertices, num_edges, elapsed_time, processing_memory_info = ray_graph_info
    print(f"Graph Processing Time: {elapsed_time:.2f} seconds")
    print(f"Graph Processing Memory Usage: {processing_memory_info.used}")
    
    # Print the result of the algorithm
    print(f"Result for {algorithm_to_execute}:\n"
          f"Number of Vertices: {num_vertices}\n"
          f"Number of Edges: {num_edges}")

    print(f"Graph Loading Time: {load_time:.2f} seconds")
    print(f"Graph Loading Memory Usage: {load_memory_info.used}")
  

    
    cpu_cores = psutil.cpu_count(logical=False)  # Number of physical CPU cores
    print(f"Number of CPU cores: {cpu_cores}")

    ray_nodes = ray.nodes()
    num_nodes = len(ray_nodes)

    print(f"Number of Ray nodes (workers): {num_nodes}")

if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3:
        print("Usage: python main.py <algorithmToExecute> <datasetSize>")
        sys.exit(1)

    algorithm_to_execute = sys.argv[1]
    dataset_size = sys.argv[2]

    main(algorithm_to_execute, dataset_size)