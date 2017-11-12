package cn.gxufe.spark.scala.sql

import java.util

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types._

object UdfAggregateFunction {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("UdfFunction")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val userAcc = Array(
      "2016-09-01,hello",
      "2016-09-01,world",
      "2016-09-01,fda",
      "2016-09-01,fda",
      "2016-09-01,fda",
      "2016-09-02,t6m",
      "2016-09-02,fdafda")
    val data = sc.parallelize(userAcc).map(x => Row(x.split(",")(0), x.split(",")(1) , x.split(",")(1).length ))
    val structFields: util.List[StructField] = new util.ArrayList[StructField]()
    structFields.add(DataTypes.createStructField("data", DataTypes.StringType, true))
    structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true))
    structFields.add(DataTypes.createStructField("length", DataTypes.IntegerType, true))
    val structType: StructType = DataTypes.createStructType(structFields)
    val df = sqlContext.createDataFrame(data,structType)
    df.registerTempTable("lengthTable")

    sqlContext.udf.register("DataAvg",new DataAvg)

    sqlContext.sql("select data,DataAvg(length) from lengthTable group by data").show()
  }
}

class DataAvg extends UserDefinedAggregateFunction{
  override def inputSchema: StructType = StructType(StructField("length", IntegerType) :: Nil)

  override def bufferSchema: StructType = StructType(StructField("sum", IntegerType) :: StructField("counter", IntegerType) :: Nil)

  override def dataType: DataType = IntegerType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0,0)
    buffer.update(1,0)
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val value = input.getAs[Integer](0)
    val sum = buffer.getAs[Integer](0)
    val counter = buffer.getAs[Integer](1)

    buffer.update( 0, value + sum )
    buffer.update( 1, (counter + 1) )
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = { // 分区合并
    val sum1 = buffer1.getAs[Integer](0)
    val counter1 = buffer1.getAs[Integer](1)
    val sum2 = buffer2.getAs[Integer](0)
    val counter2 = buffer2.getAs[Integer](1)
    buffer1.update( 0,  sum1 + sum2)
    buffer1.update( 1,  counter1 + counter2)
  }

  override def evaluate(buffer: Row): Any = {
    val sum = buffer.getAs[Integer](0)
    val counter = buffer.getAs[Integer](1)
    (sum / counter)
  }
}
