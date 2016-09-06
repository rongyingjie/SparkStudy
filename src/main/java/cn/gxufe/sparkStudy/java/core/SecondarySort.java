package cn.gxufe.sparkStudy.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.function.Consumer;

/**
 * @author 燕赤侠
 * @create 2016-09-06
 */
public class SecondarySort {


    public static class SortKey implements Comparable<SortKey>,java.io.Serializable{

        private int first;
        private int secondary;


        public SortKey(int first, int secondary) {
            this.first = first;
            this.secondary = secondary;
        }

        public int getSecondary() {
            return secondary;
        }

        public void setSecondary(int secondary) {
            this.secondary = secondary;
        }

        public int getFirst() {
            return first;
        }

        public void setFirst(int first) {
            this.first = first;
        }

        public int compareTo(SortKey o) {
            if(this.first > o.first){
                return 1;
            }else if(this.first == o.first){

                if(this.secondary == o.secondary){
                    return 0;
                }else if (this.secondary > o.secondary){
                    return 1;
                }else{
                    return -1;
                }

            }else{
                return -1;
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SortKey sortKey = (SortKey) o;
            if (first != sortKey.first) return false;
            if (secondary != sortKey.secondary) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = first;
            result = 31 * result + secondary;
            return result;
        }

        @Override
        public String toString() {
            return "SortKey{" +
                    "first=" + first +
                    ", secondary=" + secondary +
                    '}';
        }
    }

    public static void main(String[] args) {

        JavaSparkContext sc = null;
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCount");
        sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = sc.textFile("E:\\softPackage\\spark-1.6.1-bin-hadoop2.6\\SecondarySort.txt");

        lines.map(new Function<String, SortKey>() {
            public SortKey call(String v1) throws Exception {
                String[] split = v1.split(",");
                return new SortKey(Integer.parseInt(split[0].trim()),Integer.parseInt(split[1].trim()));
            }
        }).mapToPair(new PairFunction<SortKey, SortKey, Integer>() {
            @Override
            public Tuple2<SortKey, Integer> call(SortKey sortKey) throws Exception {
                return new Tuple2<SortKey, Integer>(sortKey,1);
            }
        }).sortByKey().collect().forEach(new Consumer<Tuple2<SortKey, Integer>>() {
            @Override
            public void accept(Tuple2<SortKey, Integer> t) {
                System.out.println(t._1());
            }
        });

        sc.stop();
    }



}
