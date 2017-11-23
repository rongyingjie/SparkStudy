package cn.gxufe.spark.scala.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

object WordCountKafka {

  def main(args: Array[String]): Unit = {
    // KafkaUtils.createDirectStream(null,String.class,Integer.class)
    val conf = new SparkConf().setMaster("local[4]").setAppName("WordCountStateByKey")
    val ssc = new StreamingContext(conf, Seconds(3))

    
  }

}
