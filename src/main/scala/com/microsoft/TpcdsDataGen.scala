package com.microsoft

import org.apache.spark.sql.{SQLContext, SparkSession}

/**
 * Usage: TpcdsDataGen [dataDir] [scaleFactor] [partition] [table] [overwrite]
 */
object TpcdsDataGen {
  def main(args: Array[String]) {
    if (args.length < 3) {
      printUsage()
      System.exit(1)
    }

    val spark = SparkSession
      .builder
      .appName("TPC-DS Data Gen")
      .getOrCreate()

    val dataDir = args(0) // Directory contains tpcds data in parquet format
    val scaleFactor = args(1)
    val partitions = args(2)
    val table = if (args.length > 3) args(3) else ""
    val overwrite = if(args.length > 4) args(4).toBoolean else false
    val sqlContext = new SQLContext(spark.sparkContext)
    // Run:
    val tables = new TPCDSTables(sqlContext,
      dsdgenDir = "/mnt/d", // location of dsdgen
      scaleFactor = scaleFactor,
      useDoubleForDecimal = false, // true to replace DecimalType with DoubleType
      useStringForDate = false) // true to replace DateType with StringType

    tables.genData(
      location = dataDir,
      format = "parquet",
      overwrite = overwrite, // overwrite the data that is already there
      partitionTables = true, // create the partitioned fact tables
      clusterByPartitionColumns = true, // shuffle to get partitions coalesced into single files.
      filterOutNullPartitionValues = false, // true to filter out the partition with NULL key value
      tableFilter = table,
      numPartitions = partitions.toInt) // how many dsdgen partitions to run - number of input tasks.
  }

  private def printUsage(): Unit = {
    val usage = """ TpcdsDataGen
                  |Usage: dataDir scaleFactor partitions
                  |dataDir - (string) Directory contains tpcds dataset in parquet format
                  |scaleFactor - (int) Volume of data to generate in GB
                  |partitions - (int) parallelism on datagen and number of writers
                  |table - (string) Table to generate. Default all
                  |overwrite - (bool) Overwrite if existed. Default false """

    println(usage)
  }
}
