package cn.gxufe.sparkStudy.scala.core

import org.apache.spark.{SparkContext, SparkConf}


/**
 * @author 燕赤侠
 **/
object WordCountScala {


  def  main (args: Array[String]) {

    val conf = new SparkConf().setMaster("local[2]").setAppName("WordCount");
    val sc = new SparkContext(conf);

    val lines = sc.textFile("E:\\softPackage\\spark-1.6.1-bin-hadoop2.6\\README.md");

    lines.flatMap( v => v.split(" ") ).map(word =>( word,1)).reduceByKey((v1,v2) => v1+v2).foreach(
     x => println(x._1 + "\t" + x._2 )
    );

    sc.stop()

  }

}
