package com.microsoft

import org.rogach.scallop.ScallopConf

class AppConf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val exclude =
    opt[String](default = Some(""), descr = "Exclude query separated by comma. Example: q1,q5")
  val dataDir = opt[String](required = true, descr = "Directory contains tpcds dataset in parquet format")
  val resultDir = opt[String](required = true, descr = "Directory for writing benchmark result")
  val queries = opt[String](default = Some(""), descr = "Queries to run separated by comma, such as 'q4,q5'")
  val iterations = opt[Int](default = Some(1), descr = "The number of iterations for each query")
  val cbo = opt[Boolean](default = Some(false), descr = "Enable cbo")
  val sleepTime = opt[Long](default = Some(0), descr = "Sleep time in ms between each query")
  verify()

}

object AppConf {
  def main(args: Array[String]): Unit = {
    new AppConf(args).printHelp()
  }
}
