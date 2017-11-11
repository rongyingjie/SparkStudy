package cn.gxufe.spark.java.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author 燕赤侠
 * @create 2016-09-04
 *
 *  滑动窗口 wordCount
 */
public class WindowWordCount {


    public static void main(String[] args) throws Exception {


        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("WindowWordCount");

        // 这里设置3秒
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("hadoop01", 8888);

        JavaPairDStream<String, Integer> pairDStream = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        }).reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }, Durations.seconds(20), Durations.seconds(10));

        pairDStream.print();

        jssc.start();
        jssc.awaitTermination();
        jssc.stop();
    }

}
