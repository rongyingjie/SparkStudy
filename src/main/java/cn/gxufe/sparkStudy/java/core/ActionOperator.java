package cn.gxufe.sparkStudy.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * @author 燕赤侠
 * @create 2016-08-30
 */
public class ActionOperator {

    public static void main(String[] args) {

        JavaSparkContext sc = null;
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("ActionOperator");
        sc = new JavaSparkContext(sparkConf);

        reduce(sc);

        collect(sc);

        count(sc);

        take(sc);

        countByKey(sc);

        foreach(sc);

        sc.stop();


    }

    public static void reduce(JavaSparkContext sc){

        List<Integer> list =  Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        Integer reduce = sc.parallelize(list).reduce(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        System.out.println(reduce);


    }

    public static void collect(JavaSparkContext sc){
        List<Integer> list =  Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        List<Tuple2<Integer, Integer>> collect = sc.parallelize(list).mapToPair(new PairFunction<Integer, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer integer) throws Exception {
                return new Tuple2<Integer, Integer>(integer, integer * 2);
            }
        }).collect();//立即执行

        collect.forEach(new Consumer<Tuple2<Integer, Integer>>() {
            public void accept(Tuple2<Integer, Integer> t) {
                System.out.println(t);
            }
        });

    }

    public static void count(JavaSparkContext sc){
        List<Integer> list =  Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        long count = sc.parallelize(list).count();
        System.out.println(count);
    }

    public static void take(JavaSparkContext sc){
        List<Integer> list =  Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        sc.parallelize(list).take(3).forEach(new Consumer<Integer>() { // 取出前面3个数据，并输出
            public void accept(Integer integer) {
                System.out.println(integer);
            }
        });
    }

    public static void countByKey(JavaSparkContext sc){
        List<Integer> list =  Arrays.asList(1, 2, 3, 4, 5, 4, 2, 8, 1, 2);
        sc.parallelize(list).countByValue().forEach(new BiConsumer<Integer, Long>() {
            public void accept(Integer integer, Long aLong) {
                System.out.println( integer + "\t" + aLong);
            }
        });
    }

    public static void foreach(JavaSparkContext sc){

    }


}
