package com.nbs

case class HDFS(url: String)

object Config {
  val hdfs = new HDFS(url="hdfs://localhost:9000")
}
