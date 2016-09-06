package cn.gxufe.sparkStudy.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;
import scala.actors.threadpool.Arrays;
import java.io.Serializable;


/**
 * @author 燕赤侠
 * @create 2016-09-05
 */
public class AggregateByKey {


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
        }).aggregateByKey(new MyCount(), new Function2<MyCount, Integer, MyCount>() {// 第一个参数是初始值
            public MyCount call(MyCount v1, Integer v2) throws Exception { // 分区内的结果统计
                v1.setCount(v1.getCount()+1);
                v1.setSum(v1.getSum()+v2);
                return v1;
            }
        }, new Function2<MyCount, MyCount, MyCount>() {
            public MyCount call(MyCount v1, MyCount v2) throws Exception {//合并分区
                v1.setCount(v1.getCount()+v2.getCount());
                v1.setSum(v1.getSum()+v2.getSum());
                return v1;
            }
        }).foreach(new VoidFunction<Tuple2<String, MyCount>>() {
            @Override
            public void call(Tuple2<String, MyCount> t) throws Exception {
                System.out.println(t);
            }
        });

        sc.stop();

    }


    public static class MyCount implements Serializable{
        private int sum;
        private int count;

        public int getSum() {
            return sum;
        }

        public void setSum(int sum) {
            this.sum = sum;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

        @Override
        public String toString() {
            return "MyCount{" +
                    "sum=" + sum +
                    ", count=" + count +
                    '}';
        }
    }


}
