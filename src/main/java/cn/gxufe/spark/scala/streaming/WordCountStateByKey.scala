package cn.gxufe.spark.scala.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCountStateByKey {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[4]").setAppName("WordCountStateByKey")
    val ssc = new StreamingContext(conf, Seconds(3))
    val lines = ssc.socketTextStream("localhost", 9999)

    val updateFunc = (values: Seq[Int], state: Option[Counter]) => {
      val currentCount = values.sum
      val previousCount = state.getOrElse(new Counter(0))
      previousCount.add(currentCount)
      Some(previousCount)
    }

    ssc.checkpoint("/home/rongyingjie/spark/checkpoint")

    lines.flatMap( _.split(" ")).map(x => (x,1)).reduceByKey(_ + _).updateStateByKey(updateFunc).print()

    ssc.start()
    ssc.awaitTermination()

  }

}

case class Counter(var sum:Int) {
  def add(currentCount:Int):Unit = {
    this.sum = this.sum + currentCount
  }

  override def toString: String = {
    "sum = " + sum
  }
}