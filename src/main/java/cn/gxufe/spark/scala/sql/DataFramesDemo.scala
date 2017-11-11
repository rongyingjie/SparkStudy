package cn.gxufe.spark.scala.sql

import org.apache.spark.{SparkConf, SparkContext}

object DataFramesDemo {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("DataFramesDemo")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
    val df = sqlContext.read.json("/home/rongyingjie/path/spark-1.6.3-bin-hadoop2.6/examples/src/main/resources/people.json")
    df.show()
    df.printSchema()
    df.select("name").show()
    df.select(df("name"), df("age") + 1).show()
    df.filter(df("age") > 21).show()
    df.groupBy("age").count().show()

    df.registerTempTable("people")


    df.select( df.col("name") ).show()

    sqlContext.sql("select * from people").show()




  }

}
