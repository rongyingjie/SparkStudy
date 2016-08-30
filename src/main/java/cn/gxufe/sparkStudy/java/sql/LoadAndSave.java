package cn.gxufe.sparkStudy.java.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * @author 燕赤侠
 * @create 2016-08-28
 */
public class LoadAndSave {

    public static void main(String[] args) {

        JavaSparkContext sc = null;
        SQLContext sqlContext = null;

        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("LoadAndSave");
        sc = new JavaSparkContext(sparkConf);
        sqlContext = new SQLContext(sc);

        DataFrame userDf =  sqlContext.read().parquet("/sparkStudy/users.parquet");

        userDf.printSchema();

        sc.stop();

    }

}
