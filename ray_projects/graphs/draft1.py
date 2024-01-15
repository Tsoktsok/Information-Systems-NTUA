import ray
import time
import psutil
import networkx as nx

ray.init()

HDFS_BASE_URL = "hdfs://master:54310"

def load_graph(file_path):
    start_time = time.time()

    edges = [tuple(map(int, line.split()))
             for line in open(file_path).readlines()]

    elapsed_time = time.time() - start_time
    memory_info = psutil.virtual_memory()

    return edges, elapsed_time, memory_info

@ray.remote
def page_rank():
    # Load graph data
    #edges, load_time, load_memory_info = load_graph(f"{HDFS_BASE_URL}/user/user/graph_data/test.txt")
    edges, load_time, load_memory_info = load_graph("/../home/user/graph_gen/graph_19000_085.txt") #graph_31000_085.txt")     #doule4emalakia.txt")

    # Create a directed graph
    graph = nx.DiGraph(edges)

    # Perform PageRank algorithm using networkx
    start_time = time.time()
    result = nx.pagerank(graph)
    elapsed_time = time.time() - start_time

    num_vertices = graph.number_of_nodes()
    num_edges = graph.number_of_edges()

    return num_vertices, num_edges, load_time, elapsed_time, load_memory_info, psutil.virtual_memory()

@ray.remote
def connected_components():
    # Load graph data
    edges, load_time, load_memory_info = load_graph(f"{HDFS_BASE_URL}/user/user/graph_data/graph_31000_085.txt")

    # Create an undirected graph
    graph = nx.Graph(edges)

    # Perform Connected Components algorithm using networkx
    start_time = time.time()
    result = nx.connected_components(graph)
    elapsed_time = time.time() - start_time

    num_vertices = graph.number_of_nodes()
    num_edges = graph.number_of_edges()

    return num_vertices, num_edges, load_time, elapsed_time, load_memory_info, psutil.virtual_memory()

@ray.remote
def triangle_counting():
    # Load graph data
    edges, load_time, load_memory_info = load_graph(f"{HDFS_BASE_URL}/user/user/graph_data/graph_31000_085.txt")

    # Create an undirected graph
    graph = nx.Graph(edges)

    # Perform Triangle Counting algorithm using networkx
    start_time = time.time()
    result = nx.triangles(graph)
    elapsed_time = time.time() - start_time

    num_vertices = graph.number_of_nodes()
    num_edges = graph.number_of_edges()

    return num_vertices, num_edges, load_time, elapsed_time, load_memory_info, psutil.virtual_memory()

def main(algorithm_to_execute, dataset_size):
    if algorithm_to_execute not in ["page_rank", "connected_components", "triangle_counting"]:
        print("Invalid algorithm name. Supported algorithms: page_rank, connected_components, triangle_counting")
        return

    ray_graph_info = ray.get(eval(algorithm_to_execute).remote())

    # Extracting metrics
    num_vertices, num_edges, load_time, elapsed_time, load_memory_info, processing_memory_info = ray_graph_info

    print(f"Graph Loading Time: {load_time:.2f} seconds")
    print(f"Graph Loading Memory Usage: {load_memory_info.used}")
    print(f"Graph Processing Time: {elapsed_time:.2f} seconds")
    print(f"Graph Processing Memory Usage: {processing_memory_info.used}")

    # Print the result of the algorithm
    print(f"Result for {algorithm_to_execute}:\n"
          f"Number of Vertices: {num_vertices}\n"
          f"Number of Edges: {num_edges}")
    
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