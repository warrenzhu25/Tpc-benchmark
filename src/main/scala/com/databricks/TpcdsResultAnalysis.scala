package com.databricks

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

/**
 * Usage: TpcdsResultAnalysis [rawResult] [csvResult]
 */
object TpcdsResultAnalysis {
  def main(args: Array[String]) {
    if (args.length < 2) {
      printUsage()
      System.exit(1)
    }

    val spark = SparkSession
      .builder
      .appName("TPC-DS Benchmark Result analysis")
      .master("local")
      .config("spark.sql.shuffle.partitions", 1)
      .config("spark.driver.host", "localhost")
      .getOrCreate()

    val rawResult = args(0)
    val csvResult = args(1)

    analysis(spark, rawResult, csvResult)

    spark.stop()
  }

  private def analysis(spark: SparkSession, rawResult: String, csvResult: String) = {

    val result = spark.read.json(rawResult)
    val pivotColumn = "timestamp"
    val results = result.select(explode(result("results")), col(pivotColumn)).toDF("results", pivotColumn)

    results
      .withColumn("Name", col("results.name"))
      .withColumn("Time", ((col("results.parsingTime") + col("results.analysisTime") + col("results.optimizationTime") + col("results.planningTime") + col("results.executionTime")) / 1000).cast(IntegerType))
      .drop(col("results"))
      .groupBy("Name")
      .pivot(pivotColumn)
      .sum("Time")
      .coalesce(1)
      .write
      .format("csv")
      .option("header", "true")
      .mode("append")
      .save(csvResult)
  }

  private def printUsage(): Unit = {
    val usage = """ TpcdsResultAnalysis
                  |Usage: rawResult csvResult
                  |rawResult - (string) Directory contains tpcds result in json format
                  |csvResult - (string) Directory for writing benchmark result in csv"""

    println(usage)
  }
}
