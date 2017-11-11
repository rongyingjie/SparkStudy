package cn.gxufe.spark.scala.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{AccumulableParam, SparkConf, SparkContext}

import scala.collection.mutable

object WordCount {

  def main(args: Array[String]): Unit = {
    val path = "/home/rongyingjie/path/zookeeper-3.4.8/README.txt"
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCount");
    val sparkContext = new SparkContext(sparkConf)
    val lines = sparkContext.textFile(path)

    val res = lines.flatMap( line => {
      line.split(" ")
    }).map( x => (x,1) ).reduceByKey( (x,y) => x+y ).filter( t => t._2 > 2 ).collect()
    for (i <- res){
      println(i)
    }

    /**
      * createCombiner: V => C  :  V 按照key 划分分区后的第一条数据
      * mergeValue  ： 分区内的数据累加
      * mergeCombiners  ： 分区间数据累加
      */
    lines.flatMap( line => line.split(" ") ).map(word => (word,1)).combineByKey( x=>x  ,(a:Int,b:Int) => a+b ,(a:Int,b:Int) => a+b).collect()


    /**
      * zeroValue: 初始值
      * seqOp : 分区内的数据累加
      * combOp ： 分区间数据累加
      */
    lines.flatMap( line => line.split(" ") ).map(word => (word,1)).aggregateByKey(0)((a,b)=>a+b,(a,b)=>a+b).collect()


  //  mapPartitions(lines) //

  //  AccumulableParamTest(lines,sparkContext) // 使用累加实现

   // aggregate(lines)

    sparkContext.stop()
  }



  def mapPartitions(lines:RDD[String]):Unit = {
    lines.mapPartitions(line => {
      var map = Map[String,Int]()
      while (line.hasNext){
        val words = line.next().split(" ")
        val it = words.iterator
        while(it.hasNext){
          val word = it.next()
          var count = map.getOrElse(word,0)
          count += 1
          map += (word -> count)
        }
      }
      map.iterator
    }).map( it => {
      (it._1,it._2)
    } ).reduceByKey(_+_).filter( t => t._2 > 2).foreach( println )

  }

  def AccumulableParamTest(lines:RDD[String],sc:SparkContext):Unit = {
    val accumuMap = sc.accumulable(new mutable.HashMap[String,Int])(new MapAccumulatorParam)
    lines.foreachPartition( f => {
      f.foreach( line => {
        accumuMap.add(line)
      } )
    })
    val result = accumuMap.value
    result.filter( f => f._2 > 2 ).foreach( println )
  }

  def aggregate(lines:RDD[String]) :Unit = {
    lines.aggregate(new mutable.HashMap[String,Int]())( ( item:mutable.HashMap[String,Int],line:String  ) => {
      val words = line.split(" ")
      for( word <- words ){
        val count = item.getOrElse(word,0)
        item(word) = (count+1)
      }
      item
    },( i1:mutable.HashMap[String,Int] ,i2:mutable.HashMap[String,Int] ) => {
      for ( item <- i1 ){
        val c1 = i1.getOrElse(item._1,0)
        val c2 = i2.getOrElse(item._1,0)
        i2(item._1) = c2 + c1
      }
      i2
    } ).filter( item => item._2 > 2 ).foreach( println )
  }
}


class MapAccumulatorParam extends AccumulableParam[mutable.HashMap[String,Int],String]{

  override def addInPlace(r1: mutable.HashMap[String, Int], r2: mutable.HashMap[String, Int]): mutable.HashMap[String, Int] = {
    for( r2Item <- r2){
      val r1Count = r1.getOrElse( r2Item._1,0 )
      val r2Count = r2.getOrElse( r2Item._1,0 )
      r1(r2Item._1) = (r1Count + r2Count)
    }
    r1
  }
  override def zero(initialValue: mutable.HashMap[String, Int]):mutable.HashMap[String, Int] = {
    new mutable.HashMap[String, Int]()
  }
  override def addAccumulator(r: mutable.HashMap[String, Int], line: String) = {
    val it = line.split(" ").iterator
    while(it.hasNext){
      val word = it.next()
      var count = r.getOrElse(word,0)
      count = count + 1
      r(word) = count
    }
    r
  }
}




