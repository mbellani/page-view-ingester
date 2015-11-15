package com.nbs

import java.util.concurrent.TimeUnit

import backtype.storm.utils.Utils
import backtype.storm.{Config, LocalCluster}

object LocalTopologyRunner {
  def main(args: Array[String]): Unit = {
    val localCluster = new LocalCluster()
    val topology = StormTopologyBuilder.build()

    localCluster.submitTopology("page-view-ingester", config, topology)
    Utils.sleep(TimeUnit.MINUTES.toMillis(10))
    localCluster.killTopology("page-view-ingester")
  }

  private def config(): Config = {
    val config = new Config
    config.setDebug(true)
    config
  }
}
