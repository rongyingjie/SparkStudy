package cn.gxufe.spark.scala.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[4]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(3))
    val lines = ssc.socketTextStream("localhost", 9999)
    lines.flatMap( _.split(" ")).map(x => (x,1)).reduceByKey(_ + _).print()
    ssc.start()
    ssc.awaitTermination()

  }

}
