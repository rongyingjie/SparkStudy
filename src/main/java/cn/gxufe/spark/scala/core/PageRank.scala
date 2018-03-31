package cn.gxufe.spark.scala.core

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/*

A:0|0.2 B,C,D
B:0|0.2 D,E
C:0|0.2 E
D:0|0.2 E
E:0|0.2 A

参考图例：
  https://www.cnblogs.com/rubinorth/p/5799848.html

 */
object PageRank {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("PageRank")
    val sc = new SparkContext(conf)

    var ranks = sc.textFile("src/main/resources/page_rank.txt")

    for ( i <- 1 to 50 ) {
      ranks = ranks.flatMap(line => {
        val arrayBuffer = ArrayBuffer[Tuple2[String, String]]()
        val split = line.split(":")
        val rankAndNeighbors = split(1).split("\\s+")
        val rank = rankAndNeighbors(0).split("\\|")(1).toDouble
        arrayBuffer += new Tuple2(split(0).trim, split(1))
        if (rankAndNeighbors.length > 1) {
          val neighbors = rankAndNeighbors(1).split(",")
          neighbors.map(neighbor => {
            arrayBuffer += new Tuple2(neighbor.trim, "1|" + (rank / neighbors.length).toString)
          })
        }
        arrayBuffer.toList
      }).groupByKey().map(
        item => {
          val key = item._1
          val iterator = item._2.iterator
          var neighbors = ""
          var rank = 0.0
          while (iterator.hasNext) {
            val next = iterator.next()
            val split = next.split("\\|")
            if (split(0).equals("0") && split.length == 2) {
              val tmp = split(1).split("\\s+")
              if (tmp.length == 2) {
                neighbors = tmp(1)
              }
            } else {
              rank += split(1).toDouble
            }
          }
          if (rank.equals(0.0)) {
            key + ":0|" + rank
          } else {
            key + ":0|" + rank + " " + neighbors.trim
          }
        }
      )
    }
    ranks.foreach( println )
    sc.stop()

  }

}
