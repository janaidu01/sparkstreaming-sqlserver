package com.spark

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Vivek on 7/23/2017.
  */
object ScalaRunner {

  def main(args: Array[String]): Unit = {
    val ssc = new StreamingContext("local[*]", "SqlServer", Seconds(5))
    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")
    val topics = List("v").toSet
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics).map(_._2)

    lines.foreachRDD { rdd =>
      rdd.collect().foreach{ record =>

        // Create a connection and save data. This can be optimized
        // by creating a connection pool or using rdd.foreachPartition.
        val con = new DBCon()
        con.createCon()
        con.save(record)
        con.close()
      }
    }

    ssc.checkpoint("C:/temp/")
    ssc.start()
    ssc.awaitTermination()
  }


}
