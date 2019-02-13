package com.spark.test.spark

import com.spark.test.domain.ClickLog
import com.spark.test.utils.DateUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Kafka connect à SparkStreaming
  */
object KafkaStreamingApp {
  def main(args: Array[String]): Unit = {
    if(args.length!=4){
      System.err.println("Usage: <zkQuorum> <group> <topics> <numThreads>")

    }
    val Array(zkQuorum,group,topics,numThreads) = args

    val sparkConf = new SparkConf().setAppName("KafkaReciever")
      .setMaster("local[*]")
    val ssc=new StreamingContext(sparkConf,Seconds(5))
    val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap

    val messages = KafkaUtils.createStream(ssc,zkQuorum,group,topicMap)
    //messages.map(_._2).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print
    //messages.map(_._2).count()

    val logs = messages.map(_._2)
    //data  exemple : 143.10.132.187  2017-10-27 14:26:05  "GET /class/130.html HTTP/1.1" 500  http://www.google.com

    val cleanData = logs.map(line=>{
      val infos = line.split("\t")  // infos(2)="GET /class/130.html HTTP/1.1"
      val url = infos(2).split(" ")(1)  //  /class/130.html
      val courseId = 0
      if(url.startsWith("/class")){
        val courseIdHtml=url.split("/")(2)
        val coursId=courseIdHtml.substring(0,courseIdHtml.lastIndexOf(".")).toInt
      }

      ClickLog(infos(0),DateUtils.parseToMinute(infos(1)),courseId,infos(3).toInt, infos(4))
    }).filter(log=>log.courseId!=0)

    cleanData.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
