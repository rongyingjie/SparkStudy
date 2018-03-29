package cn.gxufe.spark.scala.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 极差和中程数
  */
object MaximumAndMiddleRange {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("MaximumAndMiddleRange")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize( 1 to 20001, 10 )

    val seqOp = (mm:MaxMin , v:Int) => {
      if ( mm.max < v){
        mm.max = v
      }
      if (mm.min > v){
        mm.min = v
      }
      mm
    }

    val combOp = (v1:MaxMin,v2:MaxMin) => {
      if( v1.max < v2.min ){
        v1.max = v2.max
      }else if( v1.max < v2.max ){
        v1.max = v2.max
      }
      if(v1.min > v2.max){
        v1.min = v2.min
      }else if(v1.min > v2.min){
        v1.min = v2.min
      }
      v1
    }

    val maxMin = rdd.aggregate(new MaxMin(Int.MinValue,Int.MaxValue))(seqOp,combOp)

    println(maxMin)

    sc.stop()

  }
}


case class MaxMin(var max:Int,var min:Int)
{

  def middleRangeNumber():Int = {
    (this.max + this.min)/2
  }

  def maxiMin():Int = {
    this.max -  this.min
  }

}