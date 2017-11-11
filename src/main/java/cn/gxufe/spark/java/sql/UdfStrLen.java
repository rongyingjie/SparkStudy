package cn.gxufe.spark.java.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import java.util.ArrayList;
import java.util.List;

/**
 * @author 燕赤侠
 * @create 2016-09-03
 */
public class UdfStrLen {

    public static void main(String[] args) {

        JavaSparkContext sc = null;
        SQLContext sqlContext = null;

        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("UdfStrLen");
        sc = new JavaSparkContext(sparkConf);
        sqlContext = new SQLContext(sc);

        JavaRDD<String> lines = sc.textFile("E:\\softPackage\\spark-1.6.1-bin-hadoop2.6\\examples\\src\\main\\resources\\people.txt");

        JavaRDD<Row> peopleRow = lines.map(new Function<String, Row>() {
            @Override
            public Row call(String line) throws Exception {
                String[] split = line.split(",");
                return RowFactory.create(split[0].trim(), Integer.valueOf(split[1].trim()));
            }
        });

        //创建 peopleRow的元数据字段信息
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        StructType structType = DataTypes.createStructType(structFields);
        DataFrame peopleDf = sqlContext.createDataFrame(peopleRow, structType);

        peopleDf.registerTempTable("people");


        //注册函数
        sqlContext.udf().register("strLen", new UDF1<String,Integer>(){
           public Integer call(String input) {
               return input.length();
           }},DataTypes.IntegerType);


        sqlContext.sql("select name,age,strLen(name) as nameLen from people ").show();


        sc.stop();

    }

}
