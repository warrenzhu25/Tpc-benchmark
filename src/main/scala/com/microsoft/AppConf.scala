package com.microsoft

import org.rogach.scallop.ScallopConf

class AppConf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val exclude =
    opt[String](default = Some(""), descr = "Exclude query separated by comma. Example: q1,q5")

  val dataDir = opt[String](required = true)
  val resultDir = opt[String](required = true)
  val queries = opt[String](default = Some(""))
  val iterations = opt[Int](default = Some(1))
  val cbo = opt[Boolean]()
  verify()
}

object AppConf {
  def main(args: Array[String]): Unit = {
    new AppConf(args).printHelp()
  }
}
