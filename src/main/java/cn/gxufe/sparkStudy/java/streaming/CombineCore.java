package cn.gxufe.sparkStudy.java.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.List;

/**
 * @author 燕赤侠
 * @create 2016-09-06
 */
public class CombineCore {
    public static void main(String[] args) throws Exception {


        SparkConf conf = new SparkConf()
                .setMaster("local[2]")//至少启动2个线程
                .setAppName("CombineCore");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // 这里设置3秒
        JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(3));

        List<String> list = new ArrayList<>();
        list.add("tom");
        list.add("jack");
        final  JavaPairRDD<String, String> blacklist = sc.parallelize(list).mapToPair(new PairFunction<String, String, String>() {
            public Tuple2<String, String> call(String s) throws Exception {
                return new Tuple2<String, String>(s,"1");
            }
        });

        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("hadoop01", 8888);

        JavaPairDStream<String, String> javaPairDStream = lines.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                String[] ss = s.split(" ");
                return new Tuple2<String, String>(ss[0], ss[1]);
            }
        });

        javaPairDStream.transformToPair(new Function2<JavaPairRDD<String, String>, Time, JavaPairRDD<String, String>>() {
            public JavaPairRDD<String, String> call(JavaPairRDD<String, String> v1, Time v2) throws Exception {
                JavaPairRDD<String, String> subtract = v1.subtractByKey(blacklist);
                return subtract;
            }
        }).print();


        jssc.start();
        jssc.awaitTermination();
        jssc.stop();

    }


}
