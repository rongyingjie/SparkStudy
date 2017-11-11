package cn.gxufe.spark.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;
import scala.actors.threadpool.Arrays;

import java.util.Iterator;

/**
 * @author 燕赤侠
 * @create 2016-08-30
 *
 *      单词计数
 */
public class WordCount {

    public static void main(String[] args) {

        JavaSparkContext sc = null;
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCount");
        sc = new JavaSparkContext(sparkConf);


        JavaRDD<String> lines = sc.textFile("E:\\softPackage\\spark-1.6.1-bin-hadoop2.6\\README.md");

        lines.flatMap(new FlatMapFunction<String, String>() {

            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {

            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s,1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        }).foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {

            public void call(Iterator<Tuple2<String, Integer>> t) throws Exception {
                while (t.hasNext()){
                    System.out.println(t.next());
                }
            }
        });


        sc.stop();

    }


}
