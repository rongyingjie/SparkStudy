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
 *      DataFrame 多表联查
 *
 *  错误：
 *      System memory 259522560 must be at least 4.718592E8. Please use a larger heap size.
 *  解决方法：VM options 设置为：-Xms256m -Xmx1024m 或者更高
 */
public class DataFrameJoin {


    public static void main(String[] args){

        JavaSparkContext sc = null;
        SQLContext sqlContext = null;
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("DataFrameCreate");
        sc = new JavaSparkContext(sparkConf);
        sqlContext = new SQLContext(sc);

        DataFrame dataFrameInfo = sqlContext.read().json("E:\\softPackage\\spark-1.6.1-bin-hadoop2.6\\examples\\src\\main\\resources\\people.json");
        dataFrameInfo.registerTempTable("people_info");//注册表名

        DataFrame dataFrameScore = sqlContext.read().json("E:\\softPackage\\spark-1.6.1-bin-hadoop2.6\\examples\\src\\main\\resources\\people_score.json");
        dataFrameScore.registerTempTable("people_score");//注册表名

        sqlContext.sql("select * from people_info").show();
        sqlContext.sql("select * from people_score").show();
        sqlContext.sql("select t1.name,t2.score from people_info t1 ,people_score t2 where t1.name = t2.name and t2.score >= 80").show();

        sc.stop();

    }


}
