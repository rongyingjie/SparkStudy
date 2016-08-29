package cn.gxufe.sparkStudy.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author 燕赤侠
 * @create 2016-08-29
 */
public class JdbcDataFrame {

    public static void main(String[] args) {

        JavaSparkContext sc = null;
        SQLContext sqlContext = null;
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("JdbcDataFrame");
        sc = new JavaSparkContext(sparkConf);
        sqlContext = new SQLContext(sc);
        Map<String,String> options = new HashMap<>();
        options.put("url", "jdbc:mysql://127.0.0.1:3306/test");
        options.put("user", "root");
        options.put("password", "123456");
        options.put("dbtable", "people");
        DataFrame dataFramePeople = sqlContext.read().format("jdbc").options(options).load();
        dataFramePeople.registerTempTable("people");
        dataFramePeople.show();
        options.put("dbtable", "people_score");
        DataFrame dataFramePeopleScore = sqlContext.read().format("jdbc").options(options).load();
        dataFramePeopleScore.registerTempTable("people_score");
        dataFramePeopleScore.show();

        sqlContext.sql("select t1.name,t1.age,t2.score from people t1 , people_score t2 where t1.name = t2.name ").show();


        JavaPairRDD<String,Integer> peopleRdd = dataFramePeople.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            public Tuple2<String, Integer> call(Row row) throws Exception {
                String name = row.getAs("name");
                Integer age = row.getAs("age");
                return new Tuple2<String, Integer>(name,age);
            }
        });

        JavaPairRDD<String,Integer>  peopleScoreRdd = dataFramePeopleScore.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                String name =   row.getAs("name");
                Integer score = row.getAs("score");
                return new Tuple2<String, Integer>(name,score);
            }
        });

        JavaRDD<Row> result = peopleRdd.join(peopleScoreRdd).map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Row>() {
            public Row call(Tuple2<String, Tuple2<Integer, Integer>> v1) throws Exception {
                return RowFactory.create(v1._1(),v1._2()._1(),v1._2()._2());
            }
        });

        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        fields.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));
        fields.add(DataTypes.createStructField("score",DataTypes.IntegerType,true));
        StructType structType = DataTypes.createStructType(fields);

        DataFrame resuleDf = sqlContext.createDataFrame(result, structType);


        options.put("dbtable", "people_result");

        resuleDf.write().format("jdbc").options(options).saveAsTable("people_result");

        sc.stop();

    }
}
