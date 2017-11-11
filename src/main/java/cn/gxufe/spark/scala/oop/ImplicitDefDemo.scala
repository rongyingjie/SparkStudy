package cn.gxufe.spark.scala.oop

object ImplicitDefDemo {

  implicit def strToInt(str: String) = str.toInt


  def main(args: Array[String]): Unit = {


    val max = Math.max(1,"3") // "3" 自动调用　strToInt　方法

    println(max)

  }


}

