import sbt.Keys._
import sbtassembly.AssemblyPlugin.autoImport._

name := "graphs"

version := "1.0"

scalaVersion := "2.12.18"

val sparkVersion = "3.5.0"

libraryDependencies += "org.apache.spark"    %% "spark-sql"        % sparkVersion
libraryDependencies += "org.apache.spark"    %% "spark-core"       % sparkVersion
libraryDependencies += "org.apache.spark"    %% "spark-mllib"      % sparkVersion
libraryDependencies += "org.apache.spark"    %% "spark-graphx"     % sparkVersion
libraryDependencies += "ch.cern.sparkmeasure" %% "spark-measure" % "0.23"
libraryDependencies += "io.dropwizard.metrics" % "metrics-core" % "4.1.17"

mainClass in assembly := Some("com.graphs.Main")

assemblyMergeStrategy in assembly :={
    case PathList("META-INF", xs @ _*)=> MergeStrategy.discard
    case x => MergeStrategy.first
}

