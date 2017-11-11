package cn.gxufe.spark.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

/**
 * @author 燕赤侠
 * @create 2016-09-02
 */
public class ZipWithIndex {

    public static void main(String[] args) {

        JavaSparkContext sc = null;
        SQLContext sqlContext = null;

        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("RowNumber");
        sc = new JavaSparkContext(sparkConf);
        sqlContext = new SQLContext(sc);

        JavaRDD<String> lines = sc.textFile("E:\\softPackage\\spark-1.6.1-bin-hadoop2.6\\examples\\src\\main\\resources\\people.txt");

        lines.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] ss =  s.split(",");
                return new Tuple2<String, Integer>(ss[0],Integer.parseInt(ss[1].trim()));
            }
        }).groupByKey()
                .zipWithIndex() // 给每个key添加一个编号
                .foreach(new VoidFunction<Tuple2<Tuple2<String, Iterable<Integer>>, Long>>() {
            public void call(Tuple2<Tuple2<String, Iterable<Integer>>, Long> t) throws Exception {
                System.out.println("key = " + t._1());
                System.out.println(t._2());
            }
        });


        sc.stop();

    }
}
