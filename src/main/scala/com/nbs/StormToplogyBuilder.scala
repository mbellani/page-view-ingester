package com.nbs

import java.util

import backtype.storm.generated.StormTopology
import backtype.storm.task.TopologyContext
import backtype.storm.topology.TopologyBuilder
import org.apache.storm.hdfs.bolt.HdfsBolt
import org.apache.storm.hdfs.bolt.format.{DelimitedRecordFormat, FileNameFormat}
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy.TimeUnit
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy

object StormTopologyBuilder {


  def build(): StormTopology = {
    val builder = new TopologyBuilder

    builder.setSpout("wikipedia-page-views-spout", new WikipediaSpout)
    builder.setBolt("page-views-hdfs-bolt", hdfsBolt)
      .shuffleGrouping("wikipedia-page-views-spout")
    builder.createTopology()
  }

  private def hdfsBolt: HdfsBolt = {
    val hdfsBolt = new HdfsBolt
    hdfsBolt.withFsUrl(Config.hdfs.url)
      .withFileNameFormat(pageCountFormat())
      .withRecordFormat(new DelimitedRecordFormat().withFieldDelimiter("|"))
      .withRotationPolicy(new TimedRotationPolicy(1F, TimeUnit.DAYS))
      .withSyncPolicy(new CountSyncPolicy(1000))
    hdfsBolt
  }

  private def pageCountFormat(): FileNameFormat = {
    new FileNameFormat {
      override def getName(rotation: Long, timeStamp: Long): String = "page-views.txt"

      override def getPath: String = s"/wikipedia"

      override def prepare(conf: util.Map[_, _], topologyContext: TopologyContext): Unit = {}
    }
  }
}
