package cn.gxufe.spark.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author 燕赤侠
 * @create 2016-09-06
 *   笛卡尔积
 */
public class Cartesian {

    public static void main(String[] args) {
        JavaSparkContext sc = null;
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("Cartesian");
        sc = new JavaSparkContext(sparkConf);

        List<Integer> list1 =  Arrays.asList(1, 2, 3, 4, 5, 6);
        JavaPairRDD<Integer, Integer> pairRDD1  = sc.parallelize(list1).mapToPair(new PairFunction<Integer, Integer, Integer>() {
            public Tuple2<Integer, Integer> call(Integer integer) throws Exception {
                int value = (int) (1 + Math.random() * (10 - 1 + 1));
                return new Tuple2<Integer, Integer>(integer, value);
            }
        });

        List<Integer> list2 =  Arrays.asList(5, 6, 7, 8, 9, 10);
        JavaPairRDD<Integer, Integer> pairRDD2 = sc.parallelize(list2).mapToPair(new PairFunction<Integer, Integer, Integer>() {
            public Tuple2<Integer, Integer> call(Integer integer) throws Exception {
                int value = (int) (1 + Math.random() * (10 - 1 + 1));
                return new Tuple2<Integer, Integer>(integer, value);
            }
        });

        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> cartesian = pairRDD1.cartesian(pairRDD2);


        cartesian.foreach(new VoidFunction<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>>() {
            public void call(Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> t) throws Exception {
                System.out.println(t);
            }
        });



        sc.stop();
    }
}
