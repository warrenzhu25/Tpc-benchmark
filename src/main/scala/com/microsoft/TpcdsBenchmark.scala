package com.microsoft

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext

object TpcdsBenchmark {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("TPC-DS Benchmark")
      .config("spark.master", "local")
      .getOrCreate()

    // Set:
    val rootDir = "/mnt/d/tpcds" // root directory of location to create data in.
    val databaseName = "tpcds" // name of database to create.
    val scaleFactor = "1" // scaleFactor defines the size of the dataset to generate (in GB).
    val format = "parquet" // valid spark format like parquet "parquet".
    val sqlContext = new SQLContext(spark.sparkContext)
    // Run:
    val tables = new TPCDSTables(sqlContext,
      dsdgenDir = "/mnt/d", // location of dsdgen
      scaleFactor = scaleFactor,
      useDoubleForDecimal = false, // true to replace DecimalType with DoubleType
      useStringForDate = false) // true to replace DateType with StringType

//    tables.genData(
//      location = rootDir,
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
    tables.createExternalTables(rootDir, "parquet", databaseName, overwrite = true, discoverPartitions = true)
    // Or, if you want to create temporary tables
    // tables.createTemporaryTables(location, format)

    // For CBO only, gather statistics on all columns:
    tables.analyzeTables(databaseName, analyzeColumns = true)

    val tpcds = new TPCDS (sqlContext = sqlContext)
    // Set:

    val resultLocation = "/mnt/e" // place to write results
    val iterations = 1 // how many iterations of queries to run.
    val queries = tpcds.tpcds2_4Queries // queries to run.
    val timeout = 24*60*60 // timeout, in seconds.
    // Run:
    sqlContext.sql(s"use $databaseName")

    val experiment = tpcds.runExperiment(
      queries,
      iterations = iterations,
      resultLocation = resultLocation,
      forkThread = true)
    experiment.waitForFinish(timeout)
  }
}
