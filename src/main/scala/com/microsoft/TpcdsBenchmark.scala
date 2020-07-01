package com.microsoft

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext

/**
 * Usage: TpcdsBenchmark [dataDir] [result] [databaseName] [queries] [iterations]
 */
object TpcdsBenchmark {
  def main(args: Array[String]) {
    if (args.length < 2) {
      printUsage()
      System.exit(1)
    }

    val spark = SparkSession
      .builder
      .appName("TPC-DS Benchmark")
      .getOrCreate()

    val dataDir = args(0)
    val resultDir = args(1)
    val databaseName = if (args.length > 2) args(2) else "tpcds"
    val queries = if (args.length > 3) args(3) else ""
    val iterations = if (args.length > 4) args(4) else "1"
    val sqlContext = new SQLContext(spark.sparkContext)

    // Run:
    val tables = new TPCDSTables(sqlContext,
      scaleFactor = "1",
      useDoubleForDecimal = false, // true to replace DecimalType with DoubleType
      useStringForDate = false) // true to replace DateType with StringType

    println(s"Cost base optimization is enabled. Create external table")
    // Create the specified database
    sqlContext.sql(s"create database $databaseName")
    // Create metastore tables in a specified database for your data.
    // Once tables are created, the current database will be switched to the specified database.
    tables.createExternalTables(dataDir, "parquet", databaseName, overwrite = true, discoverPartitions = true)
    // Create temporary tables to avoid concurrent benchmark run affecting each other
    // tables.createTemporaryTables(dataDir, "parquet")

    // For CBO only, gather statistics on all columns:
    tables.analyzeTables(databaseName, analyzeColumns = true)
    val tpcds = new TPCDS(sqlContext = sqlContext)

    val queriesToRun = if (queries.isEmpty || queries == "all") {
      tpcds.tpcds2_4Queries
    } else {
      val queryNames = queries.split(",").toSet
      tpcds.tpcds2_4QueriesMap.filterKeys(queryNames.contains).values.toSeq
    } // queries to run.
    val timeout = 48 * 60 * 60 // timeout, in seconds.

    val experiment = tpcds.runExperiment(
      queriesToRun,
      iterations = iterations.toInt,
      resultLocation = resultDir,
      forkThread = true)
    experiment.waitForFinish(timeout)
  }

  private def printUsage(): Unit = {
    val usage = """ TpcdsBenchmark
                  |Usage: dataDir resultDir
                  |dataDir - (string) Directory contains tpcds dataset in parquet format
                  |resultDir - (string) Directory for writing benchmark result
                  |databaseName - (string) database name
                  |queries - (string) Queries to run separated by comma, such as 'q4,q5'. Default all
                  |iterations - (int) The number of iterations for each query. Default 1"""

    println(usage)
  }
}
