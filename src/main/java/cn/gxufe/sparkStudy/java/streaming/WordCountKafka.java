package cn.gxufe.sparkStudy.java.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author 燕赤侠
 * @create 2016-09-04
 */
public class WordCountKafka {

    public static void main(String[] args) throws Exception {


        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("WordCountKafka");

        // 这里设置3秒
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        Map<String, Integer> topicThreadMap = new HashMap<String, Integer>();
        topicThreadMap.put("WordCount", 2);

        JavaPairReceiverInputDStream<String, String> lines = KafkaUtils.createStream(
                jssc,
                "hadoop01:2181,hadoop02:2181,hadoop03:2181",
                "WordCountKafka",
                topicThreadMap);

        lines.flatMap(new FlatMapFunction<Tuple2<String,String>, String>() {
            public Iterable<String> call(Tuple2<String, String> t) throws Exception {
                return Arrays.asList(t._2().split(" "));
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s,1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {

            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v1;
            }
        }).print();

        jssc.start();
        jssc.awaitTermination();
        jssc.stop();


    }
}
