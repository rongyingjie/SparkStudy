package cn.gxufe.spark.scala.streaming

import org.apache.spark.{HashPartitioner, SparkConf}
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


    val updateFunc01 = (it: Iterator[(String, Seq[Int], Option[Int])]) => {
      val n = it.map( item => {
        val nowValue = item._2.sum + item._3.getOrElse(0)
        (item._1,nowValue)
      } )
      n
    }


    ssc.checkpoint("/home/rongyingjie/spark/checkpoint")

  //  lines.flatMap( _.split(" ")).map(x => (x,1)).reduceByKey(_ + _).updateStateByKey(updateFunc).print()

    lines.flatMap( _.split(" ")).map(x => (x,1)).reduceByKey(_ + _).updateStateByKey(updateFunc01,new HashPartitioner(3),true).print()


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