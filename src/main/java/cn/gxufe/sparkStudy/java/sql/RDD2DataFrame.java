package cn.gxufe.sparkStudy.java.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * @author 燕赤侠
 * @create 2016-08-28
 * 错误：
 *      System memory 259522560 must be at least 4.718592E8. Please use a larger heap size.
 *  解决方法：VM options 设置为：-Xms256m -Xmx1024m 或者更高
 */
public class RDD2DataFrame implements java.io.Serializable{


    public static void main(String[] args){
        JavaSparkContext sc = null;
        SQLContext sqlContext = null;

        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("RDD2DataFrame");
        sc = new JavaSparkContext(sparkConf);
        sqlContext = new SQLContext(sc);
        /**
             Michael, 29
             Andy, 30
             Justin, 19
         **/
       JavaRDD<String> lines =  sc.textFile("E:\\softPackage\\spark-1.6.1-bin-hadoop2.6\\examples\\src\\main\\resources\\people.txt");

       JavaRDD<Row> peopleRow =  lines.map(new Function<String, Row>() {
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

        peopleDf.show();

        sc.stop();
    }

}
