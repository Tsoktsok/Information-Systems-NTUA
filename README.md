# Information-Systems-NTUA
 
Project for "Information Systems" course @ NTUA (9th semester)

# Group Members:

- Konstantinos Tsokas el19073
- Ioanna Kioura el19102

# Comparison between Python scaling frameworks for big data analysis and ML

This project explores the performance and scalability of Python-based frameworks for big data analysis and machine learning, focusing on a comparison between Ray and Spark. Through detailed benchmarks and analysis, we aim to provide insights into the strengths and limitations of each framework, assisting users in making informed decisions based on their specific use cases.

# Project Overview

## Architecture

To conduct a comprehensive comparison, we established both 2 and 3-node cluster setups, installing Apache Spark, Hadoop Distributed File System (Hadoop DFS), and Ray. 
Each virtual machine is isolated on a node, hosted using the Okeanos-Knossos service by GRNET. 

## Part 1: Graph Operations (Pagerank)
In the first part, we conducted a performance analysis on graph operations, specifically Pagerank. Random graphs, featuring a few million edges, were generated and tested to evaluate the speed and efficiency of the operations.

## Part 2: Machine Learning Operations
The second part involved a comprehensive comparison of machine learning operations. We trained a regression model (Random Forest) and a clustering model (K-means) using dummy data. As accuracy was not the primary concern, the models were trained to understand the performance aspects. Subsequently, substantial amounts of data (ranging from 5 to 8 GB) were provided for predictions, allowing us to assess the scalability and efficiency of the frameworks in handling large datasets.

# Setup

## Virtual Machines
To run this project, you will need to set up virtual machines. In this guide, we will demonstrate the setup using Okeanos-Knossos; however, you can use any other platform that suits your preferences. Follow the instructions in the [notebook](https://colab.research.google.com/drive/1eE5FXf78Vz0KmBK5W8d4EUvEFATrVLmr?usp=drive_link) for detailed steps on creating the required VMs. Ensure that you have the necessary dependencies installed and configurations set up as outlined in the notebook.

Troubleshooting: Ubuntu Version Upgrade
If you encounter any issues during the Ubuntu version upgrade process, we recommend following the instructions provided in this [link](https://askubuntu.com/questions/1337637/apt-cant-upgrade-packages-and-system-still-showing-16-04-xenial-sources-af) for detailed troubleshooting steps.

## Download dependencies
To simplify the installation process, we provide a convenient script named install_dependencies.sh in the repository. Before running the script, ensure it has execution permissions by executing the following command in your terminal:
```
chmod +x install_dependencies.sh
```
Once the permissions are set, execute the script using the following command:
```
./install_dependencies.sh
```
# Usage
In each project folder, you will find detailed information on how to use and run the scripts for testing the code. Please refer to the specific README or documentation within each folder for comprehensive guidance on testing and running the provided code.


