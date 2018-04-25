package cn.gxufe.spark.scala.core

import java.util.{PriorityQueue}

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object MedianGroup {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[1]").setAppName("MedianGroup")
    val sc = new SparkContext(conf)
    val arrayBuffer = ArrayBuffer[(Int,Int)]()
    for ( i<- 1 to  5){
      for ( j <- 1 to 150){
        arrayBuffer += new Tuple2(i,j)
      }
    }
    val size = 20
    val percentile = 0.7
    val rdd = sc.parallelize( arrayBuffer.toList)
    val countByKey = rdd.map( item => {
      ( item._1 +"_" + item._2/size, item._2)
    } ).countByKey()
    val map = sc.parallelize(countByKey.toList).map(
      item => {
        val split = item._1.split("_")
        (split(0).toInt,(split(1).toInt,item._2))
      }
    )
    val key2Count = map.reduceByKey( (a,b) => ( 0,a._2 + b._2) )
    val keyIndex =  key2Count.map( item => {
      ( item._1, (item._2._2 * percentile).toInt)
    } )

    val mapToIndex  = map.groupByKey().map( item => {
      val comparator: Ordering[(Int,Long)] = new Ordering[(Int,Long)]  {
        def compare(a:(Int,Long),b: (Int,Long)):Int = {
             a._1 - b._1
          }
      }
      val  priorityQueue  = new PriorityQueue[(Int,Long)](comparator)
      val iterator = item._2.iterator
      while(iterator.hasNext)
      {
       val next =  iterator.next()
        priorityQueue.add( next )
      }
      val arrayBuffer = ArrayBuffer[(Int,Long)]()
      var before: (Int,Long) = (0,0L)
      var tmp : (Int,Long) = null
      var flag  = true
      while ( flag ){
        tmp = priorityQueue.poll()
        if( tmp == null ){
          flag = false
        }else{
          tmp = ( tmp._1 , tmp._2 + before._2 )
          before = tmp
          arrayBuffer += tmp
        }
      }
      ( item._1 ,arrayBuffer.toList)
    }).join( keyIndex ).map( item => {
      val key = item._1
      val list = item._2._1
      val index = item._2._2
      var before:(Int,Long) = (0,0)
      var result : (Int,Long) = (0,0)
      list.foreach( item => {
         if( item._2 > index.toLong && before._2 < index.toLong ){
           result = ( item._1 , index - before._2)
         }
        before = item
      } )
      ( key, result )
    } ).collect().toMap

    val mapToIndexBroadcast =  sc.broadcast(mapToIndex)
    rdd.filter( item => {
      item._2 / size   ==  mapToIndexBroadcast.value(item._1)._1
    }).groupByKey().map( item => {
      val index = mapToIndexBroadcast.value(item._1)._2.toInt
      val comparator: Ordering[Int] = new Ordering[(Int)]  {
        def compare(a:Int,b: Int):Int = {
             b - a
        }
      }
      val  priorityQueue  = new PriorityQueue[Int]( index + 1, comparator)
      val iterator  = item._2.iterator
      while (iterator.hasNext ){
        val next = iterator.next()
        priorityQueue.add( next )
        if( priorityQueue.size() > index ){
          priorityQueue.poll()
        }
      }
      println( ( item._1, priorityQueue.poll() ))
      ( item._1, priorityQueue.poll() )
    } ).collect()


    sc.stop()
  }

}
