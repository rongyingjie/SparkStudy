package cn.gxufe.spark.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * @author 燕赤侠
 * @create 2016-09-05
 */
public class Collect {

    public static void main(String[] args) {
      //  intersection();
      //  distinct();
        intersection();
    }


    /**
     * 并集，将两个RDD合并为一个RDD
     */
    public static void union(){
        JavaSparkContext sc = null;
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCount");
        sc = new JavaSparkContext(sparkConf);

        List<Integer> list1 =  Arrays.asList(1, 2, 3, 4, 5, 6);
        JavaRDD<Integer> javaRDD1 = sc.parallelize(list1);

        List<Integer> list2 =  Arrays.asList(5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> javaRDD2 = sc.parallelize(list2);
        JavaRDD<Integer> union = javaRDD1.union(javaRDD2);
        union.foreach(new VoidFunction<Integer>() {
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
        sc.stop();
    }

    /**
     * 交集，去两个集合都有的数据
     */
    public static void intersection(){
        JavaSparkContext sc = null;
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCount");
        sparkConf.set("spark.ui.enabled","false");
        sc = new JavaSparkContext(sparkConf);

        List<Integer> list1 =  Arrays.asList(1, 2, 3, 4, 5, 6);
        JavaRDD<Integer> javaRDD1 = sc.parallelize(list1);

        List<Integer> list2 =  Arrays.asList(5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> javaRDD2 = sc.parallelize(list2);

        javaRDD1.intersection(javaRDD2).foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });

        sc.stop();
    }


    /**
     * 将相同的数据，只保留一份
     */
    public static void distinct(){

        JavaSparkContext sc = null;
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCount");
        sc = new JavaSparkContext(sparkConf);
        List<Integer> list1 =  Arrays.asList(1, 2, 3, 4, 5, 6,7,7,8,8,1,2);
        JavaRDD<Integer> javaRDD1 = sc.parallelize(list1);
        javaRDD1.distinct().foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });

        sc.stop();
    }


}
