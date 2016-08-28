package cn.gxufe.sparkStudy.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author 燕赤侠
 * @create 2016-08-28
 * 错误：
 *      System memory 259522560 must be at least 4.718592E8. Please use a larger heap size.
 *  解决方法：VM options 设置为：-Xms256m -Xmx1024m 或者更高
 */
public class RDD2DataFrameJavaBean implements java.io.Serializable{


    public static void main(String[] args){
        JavaSparkContext sc = null;
        SQLContext sqlContext = null;

        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("RDD2DataFrameJavaBean");
        sc = new JavaSparkContext(sparkConf);
        sqlContext = new SQLContext(sc);
        /**
             Michael, 29
             Andy, 30
             Justin, 19
         **/
       JavaRDD<String> lines =  sc.textFile("E:\\softPackage\\spark-1.6.1-bin-hadoop2.6\\examples\\src\\main\\resources\\people.txt");

        final JavaRDD<People> peopleRdd =  lines.map(new Function<String, People>() {
            public People call(String line) throws Exception {
                String[] split = line.split(",");
                return new People(split[0], Integer.valueOf(split[1].trim()));
            }
        });

        DataFrame peopleDf =  sqlContext.createDataFrame(peopleRdd, People.class);
        peopleDf.registerTempTable("people");
        sqlContext.sql("select name,age from people").show();
        //age的值，加上5
        peopleDf.select(peopleDf.col("age").plus(5)).show();
        // age > 25 的记录
        peopleDf.filter(peopleDf.col("age").gt(25)).show();


        // dataFrame 转换为 javaRDD
        JavaRDD<Row> rowPeopleRDD  = peopleDf.toJavaRDD();
        rowPeopleRDD.map(new Function<Row, People>() {
            @Override
            public People call(Row row) throws Exception {
                String name = row.getAs("name");
                Integer age = row.getAs("age");
                return new People(name,age);
            }
        }).foreach(new VoidFunction<People>() {
            public void call(People people) throws Exception {
                System.out.println(people);
            }
        });



        sc.stop();
    }

}
