package cn.gxufe.sparkStudy.scala.sql

import java.util

import org.apache.spark.sql.types.{StructType, DataTypes, StructField}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * @author 燕赤侠 2016-09-03
 */
object UdfStrLength {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("UdfStrLength");
    val sc = new SparkContext(conf);

    val sqlContext = new SQLContext(sc)


    val userAcc = Array(
      "2016-09-01,hello",
      "2016-09-01,world",
      "2016-09-01,fda",
      "2016-09-01,fda",
      "2016-09-01,fda",
      "2016-09-02,t6m",
      "2016-09-02,fdafda");

    val data = sc.parallelize(userAcc).map(x => Row(x.split(",")(0), x.split(",")(1)))
    val structFields: util.List[StructField] = new util.ArrayList[StructField]()

    structFields.add(DataTypes.createStructField("data", DataTypes.StringType, true))
    structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true))

    val structType: StructType = DataTypes.createStructType(structFields)
    val df = sqlContext.createDataFrame(data,structType)
    df.registerTempTable("strLengthTable")


    sqlContext.udf.register("strLen",(x : String) => x.length)


    sqlContext.sql("select data,name,strLen(name) len from strLengthTable").show()

    sc.stop()

  }
}
