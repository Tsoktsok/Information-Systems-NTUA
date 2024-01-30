package com.graphs

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.scheduler._
import org.apache.spark.graphx._
import ch.cern.sparkmeasure._
import org.apache.spark.sql.functions._

object Main {
   val HDFSBaseURL = "hdfs://master:54310"

    def pageRank(spark: SparkSession): (Long, Long, Long, Long, Long, Long) = {
    val sc = spark.sparkContext
    val graphFilePath = s"$HDFSBaseURL//user/user/graph_data/graph_13000_075.txt"

    // Define the maximum number of retries
    val maxRetries = 3

    // Variables to store metrics
    var loadTime = 0L
    var loadMemory = 0L
    var processTime = 0L
    var processMemory = 0L
    var numVertices = 0L
    var numEdges = 0L

    var retries = 0
    var success = false

    while (retries < maxRetries && !success) {
        try {
        // Load the edges as a graph
        val startLoadTime = System.currentTimeMillis()
        val startLoadMemory = Runtime.getRuntime.freeMemory()
        val graph = GraphLoader.edgeListFile(sc, graphFilePath)
        val endLoadTime = System.currentTimeMillis()
        val endLoadMemory = Runtime.getRuntime.freeMemory()

        // Assign values to metrics variables
        loadTime = endLoadTime - startLoadTime
        loadMemory = endLoadMemory - startLoadMemory

        // Run PageRank
        val startProcessTime = System.currentTimeMillis()
        val startProcessMemory = Runtime.getRuntime.freeMemory()
        val ranks = graph.pageRank(0.001).vertices
        val endProcessTime = System.currentTimeMillis()
        val endProcessMemory = Runtime.getRuntime.freeMemory()

        // Assign values to metrics variables
        processTime = endProcessTime - startProcessTime
        processMemory = endProcessMemory - startProcessMemory

        // Assign values to other metrics
        numVertices = graph.numVertices
        numEdges = graph.numEdges

        // If the code reaches this point, the operation was successful
        success = true
        } catch {
        case e: Exception =>
            // Log the exception or print an error message
            println(s"Error during attempt $retries: ${e.getMessage}")
            retries += 1
            // You can add a delay between retries if needed
        }
    }

    if (!success) {
        // Handle the case where all retries failed
        throw new RuntimeException("Failed to load graph after retries.")
    }

    // Print or log success message if needed
    println("Graph loaded successfully after retries.")

    // Return the metrics
    (loadTime, loadMemory, processTime, processMemory, numVertices, numEdges)
    }

    def main(args: Array[String]): Unit = {

        if (args.length < 2) {
        println("Usage: Main <algorithmToExecute> <datasetSize>")
        sys.exit(1) // Exit the program
        }

        // Parse input args
        val algorithmToExecute = args(0)
        val datasetSize = args(1).toString

        val outputFolder = s"$HDFSBaseURL//user/user/graph_results/"

        // Create a spark session and setup the Stage Metrics
        val spark = SparkSession    
        .builder
        .appName(s"$algorithmToExecute-$datasetSize")
        .config("spark.extraListeners", "ch.cern.sparkmeasure.FlightRecorderStageMetrics")
        .master("yarn")
        .getOrCreate()
        val sc = spark.sparkContext
        val stageMetrics = StageMetrics(spark)
        var graphInfo: (Long,Long, Long, Long, Long, Long) = (0,0,0,0,0,0)
        var graphInfo2: (Long,Long) = (0,0)

        // Actually run the GraphX algorithm that we are interested in
        stageMetrics.runAndMeasure {
        if (algorithmToExecute == "pageRank") {
            graphInfo = pageRank(spark)
        }
        else if (algorithmToExecute == "connectedComponents") {
           // graphInfo2 = connectedComponents(spark)
        }
        else if (algorithmToExecute == "triangleCounting") {
            //graphInfo2 = triangleCounting(spark)
        }
        }

        // Print report for us to see, not really needed - does not affect metrics
        stageMetrics.printReport()

        // Create the metrics dataframe and aggregate it
        val df = stageMetrics.createStageMetricsDF("PerfStageMetrics")
        val aggregatedDF = stageMetrics.aggregateStageMetrics("PerfStageMetrics")

        // Create a dataframe with the metrics that we are actually interested in
        var testDF = aggregatedDF.select("numStages", "elapsedTime", "executorRunTime", "executorCpuTime", "peakExecutionMemory")

        testDF = testDF.withColumn("loadTime", lit(graphInfo._1))
        testDF = testDF.withColumn("loadMemory", lit(graphInfo._2))
        testDF = testDF.withColumn("processTime", lit(graphInfo._3))
        testDF = testDF.withColumn("processMemory", lit(graphInfo._4))
        testDF = testDF.withColumn("numNodes", lit(graphInfo._5))
        testDF = testDF.withColumn("numEdges", lit(graphInfo._6))

        testDF.show()

        // Write this file to the HDFS in a results folder
        testDF.coalesce(1).write.option("header", true).mode("overwrite").csv(outputFolder)

        // The end
        spark.stop()
    }
}
