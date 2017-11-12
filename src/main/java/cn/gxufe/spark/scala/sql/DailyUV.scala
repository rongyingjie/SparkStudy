package cn.gxufe.spark.scala.sql

import java.util

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StructType, DataTypes, StructField}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.functions._

/**
 * @author 燕赤侠 2016-09-01
 */
object DailyUV {


  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[2]").setAppName("DailyUV")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val userAcc = Array(
      "2016-09-01,1",
      "2016-09-01,1",
      "2016-09-01,2",
      "2016-09-01,3",
      "2016-09-01,3",
      "2016-09-02,1",
      "2016-09-02,1")
    val data = sc.parallelize(userAcc).map(x => Row(x.split(",")(0),x.split(",")(1).toInt) )
    val structFields: util.List[StructField] = new util.ArrayList[StructField]()

    structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true))
    structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true))
    val structType: StructType = DataTypes.createStructType(structFields)
    val df = sqlContext.createDataFrame(data,structType)
    import sqlContext.implicits._
    df.registerTempTable("uv")
    df.groupBy("name").agg('name,countDistinct('age)).map(row => Row(row(1),row(2))).foreach( println )
    sc.stop()

  }


}
