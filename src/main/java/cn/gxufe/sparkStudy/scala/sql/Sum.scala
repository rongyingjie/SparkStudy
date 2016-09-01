package cn.gxufe.sparkStudy.scala.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.functions._

/**
 * @author 燕赤侠 2016-09-01
 */
object Sum {


  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("DailyUV");
    val sc = new SparkContext(conf);

    val sqlContext = new SQLContext(sc)


    val userAcc = Array(
      "2016-09-01,45",
      "2016-09-01,43",
      "2016-09-01,32",
      "2016-09-01,78",
      "2016-09-01,85",
      "2016-09-02,55",
      "2016-09-02,77");

    val data = sc.parallelize(userAcc).map(x => (x.split(",")(0), x.split(",")(1).toInt))


    import sqlContext.implicits._

    val df = sqlContext.createDataFrame(data)

    df.groupBy("_1").agg('_1,sum('_2)).show()



  }

}
