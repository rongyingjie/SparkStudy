package cn.gxufe.spark.java.streaming;

import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author 燕赤侠
 * @create 2016-09-04
 */
public class UpdateStateByKeyWordCount {
    public static void main(String[] args) throws Exception {


        SparkConf conf = new SparkConf()
                .setMaster("local[2]")//至少启动2个线程
                .setAppName("UpdateStateByKeyWordCount");

        // 这里设置3秒
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(3));

        jssc.checkpoint("hdfs://hadoop02:9000/checkpoint");


        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("hadoop01", 8888);

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
        }).updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            public Optional<Integer> call(List<Integer> v1, Optional<Integer> v2) throws Exception {
                Integer newValue=0;
                if(v2.isPresent()){
                    newValue=v2.get();
                }
                for (Integer value:v1){
                    newValue += value;
                }
                return Optional.of(newValue);
            }
        }).print();

        jssc.start();
        jssc.awaitTermination();
        jssc.stop();
    }




}
