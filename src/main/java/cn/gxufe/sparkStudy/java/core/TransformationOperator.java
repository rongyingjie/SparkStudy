package cn.gxufe.sparkStudy.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * @author 燕赤侠
 * @create 2016-08-30
 */
public class TransformationOperator {

    public static void main(String[] args) {

        JavaSparkContext sc = null;
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("TransformationOperator");
        sc = new JavaSparkContext(sparkConf);

      //  map(sc);

      //  filter(sc);

     //   flatMap(sc);

      //  groupByKey(sc);

    //    reduceByKey(sc);

     //   sortByKey(sc);

    //    join(sc);

      //  cogroup(sc);

        sc.stop();
    }


    public static class DemoMap{
        public int i;
        public int j;

        @Override
        public String toString() {
            return "DemoMap{" +
                    "i=" + i +
                    ", j=" + j +
                    '}';
        }
    }
    public static void map(JavaSparkContext sc){

       List<Integer> list =  Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        sc.parallelize(list).map(new Function<Integer, DemoMap>() {
            public DemoMap call(Integer v1) throws Exception {
                DemoMap demoMap = new DemoMap();
                demoMap.i = v1;
                demoMap.j = v1 * 2;
                return demoMap;
            }
        }).foreachPartition(new VoidFunction<Iterator<DemoMap>>() {
            public void call(Iterator<DemoMap> t) throws Exception {
                while (t.hasNext()){
                    System.out.println(t.next());
                }
            }
        });



    }

    public static void filter(JavaSparkContext sc){
        List<Integer> list =  Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        sc.parallelize(list).filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer v1) throws Exception {
                return v1%3 == 0;
            }
        }).foreachPartition(new VoidFunction<Iterator<Integer>>() {
            @Override
            public void call(Iterator<Integer> t) throws Exception {
                while (t.hasNext()){
                    System.out.println(t.next());
                }
            }
        });
    }


    public static void flatMap(JavaSparkContext sc){

        List<String> lines =  Arrays.asList("hello world"," hello me","hello you");
        sc.parallelize(lines).flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        }).foreachPartition(new VoidFunction<Iterator<String>>() {
            @Override
            public void call(Iterator<String> t) throws Exception {
                while (t.hasNext()){
                    System.out.println(t.next());
                }
            }
        });

    }

    public static void groupByKey(final JavaSparkContext sc){
        JavaRDD<String> lines = sc.textFile("E:\\softPackage\\spark-1.6.1-bin-hadoop2.6\\README.md");
        lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        }).mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                return new Tuple2<String, String>(s, UUID.randomUUID().toString());
            }
        }).groupByKey().foreachPartition(new VoidFunction<Iterator<Tuple2<String, Iterable<String>>>>() {
            @Override
            public void call(Iterator<Tuple2<String, Iterable<String>>> iterator) throws Exception {
                while (iterator.hasNext()){
                    Tuple2<String, Iterable<String>>  tuple2 =  iterator.next();
                    Iterable<String> strings = tuple2._2();
                    System.out.println("======================"+tuple2._1()+"===========================");
                    strings.forEach(new Consumer<String>() {
                        public void accept(String s) {
                            System.out.println(s);
                        }
                    });
                    System.out.println("=================================================");
                }
            }
        });


    }


    public static void reduceByKey(JavaSparkContext sc){
        JavaRDD<String> lines = sc.textFile("E:\\softPackage\\spark-1.6.1-bin-hadoop2.6\\README.md");
        lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s,1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        }).foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
            @Override
            public void call(Iterator<Tuple2<String, Integer>> iterator) throws Exception {
                while (iterator.hasNext()){
                    System.out.println(iterator.next());
                }
            }
        });
    }

    public static void sortByKey(JavaSparkContext sc){
        JavaRDD<String> lines = sc.textFile("E:\\softPackage\\spark-1.6.1-bin-hadoop2.6\\README.md");
        lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s,1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        }).mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
                return new Tuple2<Integer, String>(t._2(),t._1());
            }
        }).sortByKey(false).mapToPair(new PairFunction<Tuple2<Integer,String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> t) throws Exception {
                return new Tuple2<String, Integer>(t._2(),t._1());
            }
        }).take(5).forEach(new Consumer<Tuple2<String, Integer>>() {
            public void accept(Tuple2<String, Integer> t) {
                System.out.println(t);
            }
        });

    }

    public static void join(JavaSparkContext sc){

        List<Tuple2<String,Double>> score = Arrays.asList(
                new Tuple2<String, Double>("A",60.0),
                new Tuple2<String, Double>("B",80.4),
                new Tuple2<String, Double>("C",55.8),
                new Tuple2<String, Double>("D",77.9)
        );


        List<Tuple2<String,Integer>> age = Arrays.asList(
                new Tuple2<String, Integer>("A",22),
                new Tuple2<String, Integer>("B",23),
                new Tuple2<String, Integer>("C",19),
                new Tuple2<String, Integer>("D",24)
        );

        JavaPairRDD<String,Double> javaPairRddScore =  sc.parallelizePairs(score);
        JavaPairRDD<String,Integer> javaPairRddAge =  sc.parallelizePairs(age);
        JavaPairRDD<String,Tuple2<Double,Integer>>  result =  javaPairRddScore.join(javaPairRddAge);
        result.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Tuple2<Double, Integer>>>>() {
            @Override
            public void call(Iterator<Tuple2<String, Tuple2<Double, Integer>>> iterator) throws Exception {
                while (iterator.hasNext()){
                    System.out.println(iterator.next());
                }
            }
        });

        /**===========================
         (A,(60.0,22))
         (B,(80.4,23))
         (C,(55.8,19))
         (D,(77.9,24))
         */

    }

    public static void cogroup(JavaSparkContext sc){
        List<Tuple2<String,Double>> score = Arrays.asList(
                new Tuple2<String, Double>("A",60.0),
                new Tuple2<String, Double>("B",80.4),
                new Tuple2<String, Double>("A",55.8),
                new Tuple2<String, Double>("C",77.9)
        );


        List<Tuple2<String,Integer>> age = Arrays.asList(
                new Tuple2<String, Integer>("A",22),
                new Tuple2<String, Integer>("B",23),
                new Tuple2<String, Integer>("C",19),
                new Tuple2<String, Integer>("C",24)
        );

        JavaPairRDD<String,Double> javaPairRddScore =  sc.parallelizePairs(score);
        JavaPairRDD<String,Integer> javaPairRddAge =  sc.parallelizePairs(age);
        JavaPairRDD<String, Tuple2<Iterable<Double>, Iterable<Integer>>> cogroup = javaPairRddScore.cogroup(javaPairRddAge);

        cogroup.foreach(new VoidFunction<Tuple2<String, Tuple2<Iterable<Double>, Iterable<Integer>>>>() {
            public void call(Tuple2<String, Tuple2<Iterable<Double>, Iterable<Integer>>> t) throws Exception {

            }
        });


    }


}
