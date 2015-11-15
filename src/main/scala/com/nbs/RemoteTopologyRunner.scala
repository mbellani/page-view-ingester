package com.nbs

import backtype.storm.{Config, StormSubmitter}
import org.apache.commons.lang.StringUtils

object RemoteTopologyRunner {

  private def createConfig(debug: Boolean): Config = {
    val config = new Config()
    config.setDebug(debug)
    config.setMessageTimeoutSecs(120)
    config.setNumWorkers(1)
    config.setNumAckers(1)
    config
  }

  def main(args: Array[String]): Unit = {
    if (args.length == 0 || args.length > 2 || StringUtils.isBlank(args(0))) {
      System.out.println("Error deploying topology.")
      System.out.println("Usage: <topology name> debug(optional)")
      System.out.println("Please provide correct command-line arguments and try again.")
      sys.exit(1)
    }

    val topologyName = args(0)
    val debug: Boolean = args.length > 1 && "debug".equalsIgnoreCase(args(1))

    StormSubmitter.submitTopology(topologyName, createConfig(debug), StormTopologyBuilder.build())
  }

}
