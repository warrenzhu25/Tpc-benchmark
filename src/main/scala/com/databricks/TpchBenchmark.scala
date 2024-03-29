package com.databricks

import org.apache.spark.sql.{SQLContext, SparkSession}

/**
 * Usage:
 *
 * -c, --cbo                 Enable cbo
 * -d, --data-dir  <arg>     Directory contains tpch dataset in parquet format
 * -e, --exclude  <arg>      Exclude query separated by comma. Example: q1,q5
 * -i, --iterations  <arg>   The number of iterations for each query
 * -q, --queries  <arg>      Queries to run separated by comma, such as 'q4,q5'
 * -r, --result-dir  <arg>   Directory for writing benchmark result
 * -h, --help                Show help message
 */
object TpchBenchmark {
  def main(args: Array[String]) {
    val appConf = new AppConf(args)
    appConf.printHelp()

    val dataDir = appConf.dataDir()
    val resultDir = appConf.resultDir()
    val databaseName = "tpch"
    val queries = appConf.queries()
    val iterations = appConf.iterations()
    val sleepTimeMs = appConf.sleepTime()

    println(s"AppConf are ${appConf.summary}")

    val spark = SparkSession
      .builder
      .getOrCreate()

    val sqlContext = new SQLContext(spark.sparkContext)

    val tpch = new TPCH(sqlContext = sqlContext)
    val excludes = appConf.exclude().split(",").toSet
    val includes = queries.split(",").toSet
    val queriesToRun =
      tpch.queries
        .filter(q => queries.isEmpty || includes.contains(q.name))
        .filter(q => !excludes.contains(q.name))

    println(s"Queries to Run: ${queriesToRun.map(q => q.name).mkString(",")}")

    // Run:
    val tables = new TPCHTables(sqlContext,
      scaleFactor = "1",
      useDoubleForDecimal = false, // true to replace DecimalType with DoubleType
      useStringForDate = false) // true to replace DateType with StringType

    println(s"Cost base optimization is enabled. Create external table")
    // Create the specified database
    sqlContext.sql(s"create database $databaseName")
    // Create metastore tables in a specified database for your data.
    // Once tables are created, the current database will be switched to the specified database.
//    tables.createExternalTables(dataDir, "parquet", databaseName, overwrite = true, discoverPartitions = true)
    // Create temporary tables to avoid concurrent benchmark run affecting each other
    tables.createTemporaryTables(dataDir, "parquet")

    if(appConf.cbo()) {
      tables.analyzeTables(databaseName, analyzeColumns = true)
    }
    val timeout = 48 * 60 * 60 // timeout, in seconds.

    val experiment = tpch.runExperiment(
      queriesToRun,
      iterations = iterations.toInt,
      resultLocation = resultDir,
      forkThread = true,
      sleepTimeMs = sleepTimeMs
    )
    experiment.waitForFinish(timeout)
  }
}
