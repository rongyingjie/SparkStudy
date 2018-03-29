package cn.gxufe.spark.scala.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 中位数求法
  *
  */
object Median {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("Median")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize( 1 to 20001, 10 )
    val size = 689
    val countByKey = rdd.map( num => ( num/size, num ) ).countByKey()
    val sum = countByKey.map( map => map._2 ).sum
    val medianOffset =  sum/2  +  ( if ( sum %2 == 1  ) 1 else 0 )

    val sortedList = countByKey.map( map => {
      map._1
    } ).toList.sorted

    var preOffset = 0L
    var nowOffset = 0L
    var bucketIndex = 0

    val iterator = sortedList.iterator
    var flag = true
    while(iterator.hasNext && flag){
      val index = iterator.next()
      nowOffset = countByKey(index) + preOffset
      if ( preOffset <= medianOffset &&  medianOffset <= nowOffset){
        bucketIndex = index
        flag = false
      }
      if (flag )
          preOffset = nowOffset
    }


    println(preOffset + "," + nowOffset +"," + medianOffset + "," +bucketIndex)

    val result = rdd.filter(num =>  num /size == bucketIndex  ).sortBy( x => x ).take((medianOffset - preOffset).toInt).last
    println(result )
    sc.stop()
  }


}


