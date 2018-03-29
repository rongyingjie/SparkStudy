package cn.gxufe.spark.scala.core

import org.apache.spark.{AccumulableParam, SparkConf, SparkContext}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object Dijkstra {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("Dijkstra")
    val sc = new SparkContext(conf)
    val startPoint = "0"
    sc.textFile("src/main/resources/simple_weightedDigraph.txt").map( line => {
        val split = line.split("\\s+")
      (split(0),split(1)+","+split(2))
    } ).groupByKey().map( item => {
        val key = item._1
        val iterator = item._2.iterator
        val sb = new StringBuffer()
        if(key.equals( startPoint  )){
            sb.append(0).append("|").append(0).append("|")
        }else{
            sb.append(-1).append("|").append(1).append("|")
        }
        while (iterator.hasNext){
          sb.append( iterator.next()).append(" ")
        }
      (key,sb.toString)
    } ).saveAsTextFile("src/main/resources/tmp_0")

    val init = new mutable.HashMap[String,Double]

    val acc = sc.accumulable(init)(new DistanceAccumulatorParam)
    var index = 0
    var flag = true
    while ( flag ){
      acc.value.put("stop.flag",1)
      val preData = sc.textFile("src/main/resources/tmp_"+index).map( line => {
        val message = line.substring( 1, line.length - 1 )
        val split = message.split(",", 2)
        if ( split(1).endsWith(" ") ) {
          (split(0),split(1).substring(0, split(1).length -1 ))
        }else{
          (split(0),split(1))
        }
      } )
      index = index + 1
      preData.flatMap( item => {
        val arrayBuffer = ArrayBuffer[Tuple2[String,String]]()
        val split = item._2.split("\\|")
        if(split(1).equals("0")){
          acc.add(( item._1, split(0).toDouble))
          if(split.length == 3){
            val points = split(2).split("\\s+")
            points.map(  point => {
              val kv = point.split(",")
              acc.add("stop.flag", 0 )
              arrayBuffer +=  new Tuple2( kv(0), ( kv(1).toDouble + split(0).toDouble  ) + "|0 ")
            } )
          }
        }else{
          arrayBuffer += item
        }
        arrayBuffer.toList
      }).groupByKey().map( item => {
         val iterator = item._2.iterator
         val sb = new mutable.StringBuilder()
         var flag = false
         var dis = Double.MaxValue
         while(iterator.hasNext){
           val next = iterator.next()
           val split = next.split("\\|")
           if(split(1).trim.equals("0")){
             flag = true
             if ( dis > split(0).toDouble) {
               dis = split(0).toDouble
             }
           }
           if( split.length > 2 ){
             sb.append(split(2)).append(" ")
           }
         }
        var process = 1
        if ( Double.MaxValue.equals(dis) ){
          dis = -1
        }
        if(flag){
          process = 0
        }
        if ( sb.length == 0 ){
          (item._1, dis +"|" + process)
        }else{
          val index = sb.toString().lastIndexOf(" ")
          (item._1, dis +"|" + process + "|"+ sb.toString().substring(0, index ) )
        }
      } ).saveAsTextFile("src/main/resources/tmp_"+index)
      if(acc.value.get("stop.flag").get.equals(1.0)){
        flag = false
      }else{
        flag = true
      }
    }
    acc.value.remove("stop.flag")
    println( acc.value )
    sc.stop()
  }
}

class DistanceAccumulatorParam extends AccumulableParam[mutable.HashMap[String,Double],Tuple2[String,Double]] {


  override def addInPlace(r1: mutable.HashMap[String, Double], r2: mutable.HashMap[String, Double]): mutable.HashMap[String, Double] = {
    r1.foreach( item => {
        val v1  = item._2
        val v2  = r2.getOrElse( item._1 , Double.MaxValue )
        if(v1 > v2 ){
          r2.put( item._1,v2 )
        }else{
          r2.put( item._1,v1 )
        }
    } )
    r2
  }

  override def addAccumulator(r: mutable.HashMap[String, Double], t: (String, Double)): mutable.HashMap[String, Double] = {
    val v = r.getOrElse( t._1,Double.MaxValue )
    if ( v > t._2 ){
      r.put( t._1,t._2 )
    }
    r
  }

  override def zero(initialValue: mutable.HashMap[String, Double]): mutable.HashMap[String, Double] = {
    initialValue
  }
}
