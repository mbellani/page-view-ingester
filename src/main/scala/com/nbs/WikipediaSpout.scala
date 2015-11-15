package com.nbs

import java.io.{InputStream, InputStreamReader}
import java.util

import backtype.storm.spout.SpoutOutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichSpout
import backtype.storm.tuple.Fields
import com.github.tototoshi.csv.{CSVReader, DefaultCSVFormat}

import scala.collection.JavaConverters._

class WikipediaSpout extends BaseRichSpout {

  private var outputCollector: SpoutOutputCollector = _
  private var csvReader: CSVReader = _
  private var iterator: Iterator[Seq[String]] = _

  override def open(conf: util.Map[_, _], topologyContext: TopologyContext, spoutOutputCollector: SpoutOutputCollector): Unit = {
    this.outputCollector = spoutOutputCollector
    val inputStream: InputStream = Thread.currentThread().getContextClassLoader.getResourceAsStream("pagecounts-20120101-000000")
    implicit object MyFormat extends DefaultCSVFormat {
      override val delimiter = ' '
    }
    csvReader = CSVReader.open(new InputStreamReader(inputStream))
    iterator = csvReader.iterator
  }

  override def nextTuple(): Unit = {
    if (!iterator.hasNext) {
      return
    }

    val values: Seq[String] = iterator.next()
    outputCollector.emit(values
      .toList
      .asJava
      .asInstanceOf[java.util.List[Object]])
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("language", "pageName", "nonUniqueViews", "bytesTransferred"))
  }

  override def close(): Unit = {
    csvReader.close()
  }
}
