package cn.gxufe.spark.scala.core


import org.apache.spark.{SparkConf, SparkContext}

object TransformationsActionRdd {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[1]").setAppName("TransformationsRdd")
    val sparkContext = new SparkContext(sparkConf)
    //   mapPartitionsTest01(sparkContext)
    //  mapPartitionsWithIndexTest(sparkContext)
    mapPartitionsWithIndexTest(sparkContext)
    sparkContext.stop()
  }

  def mapPartitionsTest(sc:SparkContext):Unit = {
    val rdd = sc.makeRDD(1 to 100,5)

    rdd.mapPartitions(
      x => {
        var result = List[Int]()
        var sum = 0
        while(x.hasNext){
          sum += x.next
        }
        result.::(sum).iterator
      }
    ).foreach(  println )
  }

  def mapPartitionsWithIndexTest(sc:SparkContext):Unit = {
    val rdd = sc.makeRDD(1 to 20,5)

    val s = rdd.mapPartitionsWithIndex( (index,it) => {
      var result = List[Tuple2[Int,Int]]()
      var tmp = 0
      while(it.hasNext) {
        tmp = it.next()
        result = result.+:( (tmp, index) )
      }
      result.iterator
    } ).foreach( x => x)

    println(s)
  }



  def coalesesRepartition(sc:SparkContext):Unit = {
    val rdd = sc.makeRDD(1 to 30,5)
    rdd.coalesce(3,true).mapPartitionsWithIndex((index,it) => {
      while (it.hasNext){
        println( "index = " + index + ", value = " + it.next )
      }
      it
    }).collect()
  }


}