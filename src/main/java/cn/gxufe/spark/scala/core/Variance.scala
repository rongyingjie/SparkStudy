package cn.gxufe.spark.scala.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 平均数
  */
object Variance {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("MaximumAndMiddleRange")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize( 1 to 1000, 10 )

    val seqOp = (mm:SumCount , v:Int) => {
      mm.count = mm.count + 1
      mm.sum = mm.sum + v
      mm
    }
    val combOp = (v1:SumCount,v2:SumCount) => {
      v1.add(v2)
    }
    val sumCount = rdd.aggregate(new SumCount(0,0))(seqOp,combOp)
    val avg = sumCount.age()
    val variance = rdd.map( v => {
        (v - avg) *  (v - avg)
    } ).sum()/sumCount.count.toDouble
    println(variance)
    sc.stop()
  }
}

case class SumCount(var sum:Int,var count:Int){
  def age():Double = {
    sum.toDouble / count.toDouble
  }

  def add(v2:SumCount):SumCount = {
    this.sum = this.sum + v2.sum
    this.count = this.count + v2.count
    this
  }

}