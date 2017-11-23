package cn.gxufe.spark.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author 燕赤侠
 * @create 2016-08-30
 *
 *      单词计数
 */
public class WordCount {

    public static void main(String[] args) {
        JavaSparkContext sc;
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCount");
        sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = sc.textFile("E:\\softPackage\\spark-1.6.1-bin-hadoop2.6\\README.md");
        lines.flatMap(s -> Arrays.asList(s.split(" ")) ) //
                .mapToPair(s -> new Tuple2<>(s,1)).reduceByKey((Integer v1, Integer v2) ->  v1+v2
        ).foreachPartition( t -> {
            while (t.hasNext()){
                System.out.println(t.next());
            }
        });
        sc.stop();
    }

}
