package cn.gxufe.spark.scala.sql

import org.apache.spark.{SparkConf, SparkContext}

object DatasetsDemo {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("DataFramesDemo")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
    import sqlContext.implicits._
    val person = sqlContext.read.json("/home/rongyingjie/path/spark-1.6.3-bin-hadoop2.6/examples/src/main/resources/people.json").as[Person]
    person.show()


    val pDf  = sparkContext.textFile("/home/rongyingjie/path/spark-1.6.3-bin-hadoop2.6/examples/src/main/resources/people.txt").map( x => {
      val p = x.split(",")
      new Person(p(0).trim,p(1).trim.toInt)
    } ).toDF()

    pDf.show()

  }

}

case class Person(name: String, age: Long)