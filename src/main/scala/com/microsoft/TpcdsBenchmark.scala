package com.microsoft

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext

/**
 * Usage: TpcdsBenchmark [dataDir] [result]
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
      .master("local")
      .getOrCreate()

    val dataDir = args(0)
    val resultDir = args(1)
    val databaseName = "tpcds" // name of database to create.
    val scaleFactor = "1" // scaleFactor defines the size of the dataset to generate (in GB).
    val sqlContext = new SQLContext(spark.sparkContext)
    // Run:
    val tables = new TPCDSTables(sqlContext,
      dsdgenDir = "/mnt/d", // location of dsdgen
      scaleFactor = scaleFactor,
      useDoubleForDecimal = false, // true to replace DecimalType with DoubleType
      useStringForDate = false) // true to replace DateType with StringType

//    tables.genData(
//      location = dataDir,
//      format = format,
//      overwrite = true, // overwrite the data that is already there
//      partitionTables = true, // create the partitioned fact tables
//      clusterByPartitionColumns = true, // shuffle to get partitions coalesced into single files.
//      filterOutNullPartitionValues = false, // true to filter out the partition with NULL key value
//      tableFilter = "", // "" means generate all tables
//      numPartitions = 100) // how many dsdgen partitions to run - number of input tasks.

    // Create the specified database
    sqlContext.sql(s"create database $databaseName")
    // Create metastore tables in a specified database for your data.
    // Once tables are created, the current database will be switched to the specified database.
    tables.createExternalTables(dataDir, "parquet", databaseName, overwrite = true, discoverPartitions = true)
    // Or, if you want to create temporary tables
    // tables.createTemporaryTables(location, format)

    // For CBO only, gather statistics on all columns:
    tables.analyzeTables(databaseName, analyzeColumns = true)

    val tpcds = new TPCDS (sqlContext = sqlContext)

    val iterations = 1 // how many iterations of queries to run.
    val queries = tpcds.tpcds2_4Queries // queries to run.
    val timeout = 24*60*60 // timeout, in seconds.
    // Run:
    sqlContext.sql(s"use $databaseName")

    val experiment = tpcds.runExperiment(
      queries,
      iterations = iterations,
      resultLocation = resultDir,
      forkThread = true)
    experiment.waitForFinish(timeout)
  }

  private def printUsage(): Unit = {
    val usage = """ TpcdsBenchmark
                  |Usage: dataDir resultDir
                  |dataDir - (string) Directory contains tpcds dataset in parquet format
                  |resultDir - (string) Directory for writing benchmark result"""

    println(usage)
  }
}
