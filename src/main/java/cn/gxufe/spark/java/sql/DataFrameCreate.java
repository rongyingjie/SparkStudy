package cn.gxufe.spark.java.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 *  @author 燕赤侠
 *  @create 2016-08-28
 *
 * 说明：
 *      DataFrame基本操作
 *
 *  错误：
 *      System memory 259522560 must be at least 4.718592E8. Please use a larger heap size.
 *  解决方法：VM options 设置为：-Xms256m -Xmx1024m 或者更高
 */
public class DataFrameCreate {


    public static void main(String[] args){

        JavaSparkContext sc = null;
        SQLContext sqlContext = null;
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("DataFrameCreate");
        sc = new JavaSparkContext(sparkConf);
        sqlContext = new SQLContext(sc);

        //读取spark自带的案例json文件
        /**
             {"name":"Michael"}
             {"name":"Andy", "age":30}
             {"name":"Justin", "age":19}
            {"name":"jack", "age":19} //手动添加
         **/
        DataFrame dataFrame = sqlContext.read().json("E:\\softPackage\\spark-1.6.1-bin-hadoop2.6\\examples\\src\\main\\resources\\people.json");
        dataFrame.show();//查看数据
        dataFrame.registerTempTable("people");//注册表名
        dataFrame.printSchema();//查看元数据
        sqlContext.sql("select count(1) as count from people").show();
        //查询name的值
        dataFrame.select(dataFrame.col("name")).show();
        //过滤null值
        dataFrame.filter(dataFrame.col("age").isNotNull()).show();
        // 每列age的值，都加上 5
        dataFrame.select(dataFrame.col("age").plus(5)).show();
        dataFrame.groupBy(dataFrame.col("age")).count().show();

        sc.stop();

    }


}
