package cn.gxufe.spark.scala.core

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**

1 2,3,4,5,6,7,8
2 1,3,4,5,7
3 1,2
4 1,2,6
5 1,2
6 1,4
7 1,2
8 1





  */
object RecommendFriend {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("RecommendFriend")
    val sc = new SparkContext(conf)

    val self2friends  = sc.textFile("src/main/resources/RecommendFriend.txt").map(line => {
      val split =  line.split("\\s+")
      (split(0).trim,split(1).trim)
    })



    self2friends.cartesian(self2friends).flatMap(
      item => {
        val arrayBuffer = ArrayBuffer[Tuple2[String,String]]()
        val i1 = item._1
        val i2 = item._2
        if( i1._1.equals(i2._1) ){
          arrayBuffer.toList
        } else {
          val i1Friends = i1._2.split(",")
          val i2Friends = i2._2.split(",")
          var sum = 0
          var flag = true
          for( i1i <- i1Friends){
            for( i2i <- i2Friends ){
                if (i1i.equals(i2._1) && i2i.equals(i1._1)) { // 已经是好友
                  flag = false
                }else{
                  if( i1i.equals(i2i) ){
                    sum += 1
                  }
                }
            }
          }
          if(flag  ){ // 已经是好友
            val rate = sum.toDouble / Math.sqrt( i1Friends.length * i2Friends.length )
            arrayBuffer += new Tuple2( i1._1,i2._1+":"+rate)
          }
          arrayBuffer.toList
        }
      }
    ).sortByKey().foreach( println ) // 后续 topN


    sc.stop()
  }

}
