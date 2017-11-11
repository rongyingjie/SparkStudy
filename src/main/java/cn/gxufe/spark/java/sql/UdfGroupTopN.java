package cn.gxufe.spark.java.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * 自定义函数，实现分组topN
 * @author 燕赤侠
 * @create 2016-09-02
 */
public class UdfGroupTopN {

    public static void main(String[] args) {

        JavaSparkContext sc = null;
        SQLContext sqlContext = null;

        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("UDF");
        sc = new JavaSparkContext(sparkConf);
        sqlContext = new SQLContext(sc);

        JavaRDD<String> lines = sc.textFile("E:\\softPackage\\spark-1.6.1-bin-hadoop2.6\\examples\\src\\main\\resources\\topN.txt");

        JavaRDD<Row> peopleRow = lines.map(new Function<String, Row>() {
            @Override
            public Row call(String line) throws Exception {
                String[] split = line.split(",");
                return RowFactory.create(split[0].trim(), Integer.valueOf(split[1].trim()));
            }
        });

        //创建 peopleRow的元数据字段信息
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("category", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("sale", DataTypes.IntegerType, true));
        StructType structType = DataTypes.createStructType(structFields);
        DataFrame peopleDf = sqlContext.createDataFrame(peopleRow, structType);

        peopleDf.registerTempTable("topnTable");

        //取出前四位数据
        sqlContext.udf().register("TopN",new TopN(4));

        sqlContext.sql("select category,TopN(sale) as topN  from topnTable group by category ").show();

        sc.stop();

    }

    public static class TopN extends UserDefinedAggregateFunction {

        private StructType inputSchema = null;
        private int n;

        public TopN( int n ){
            this.n = n;
            if(bufferSchema == null){
                List<StructField> structFields = new ArrayList<StructField>();
                for (int i = 1; i <= n; i++) {
                    structFields.add(DataTypes.createStructField("_"+i, DataTypes.IntegerType, true));
                }
                bufferSchema = DataTypes.createStructType(structFields);
            }
            if(inputSchema == null){
                List<StructField> structFields = new ArrayList<StructField>();
                structFields.add(DataTypes.createStructField("_1", DataTypes.IntegerType, true));
                inputSchema = DataTypes.createStructType(structFields);
            }
        }

        //输入数据类型设置
        public StructType inputSchema() {
            return inputSchema;
        }

        StructType bufferSchema = null;

        //缓冲区设置
        public StructType bufferSchema() {
            return bufferSchema;
        }

        public DataType dataType() {
            return DataTypes.StringType;
        }

        @Override
        public boolean deterministic() {
            return true;
        }


        public void initialize(MutableAggregationBuffer buffer) {
            for (int i = 0; i < n; i++) {
                    buffer.update(i,Integer.MIN_VALUE);
            }
        }

        @Override
        public void update(MutableAggregationBuffer buffer, Row input) {
            int size = buffer.size();
            for (int i=0;i<size;i++){
                if(buffer.getInt(i) < input.getInt(0) ){
                    for (int j=size -2;i> i;j--){
                        buffer.update(j+1,buffer.getInt(j));
                    }
                    buffer.update(i+1,buffer.getInt(i));
                    buffer.update(i,input.getInt(0));
                    break;
                }
            }
        }

        /**
         * 将两个数据合并
         *      1、两个分区的加入到 list集合中
         *      2、使用冒泡排序，并将前N个数据，作为下次合并数据的输入
         *
         * @param buffer : 分区数据，同时保存结果数据
         * @param row ： 分区数据
         */
        public void merge(MutableAggregationBuffer buffer, Row row) {

            List<Integer> list = new ArrayList<>(buffer.size() * 2);
            for (int i = 0; i < buffer.size(); i++) {
                list.add(buffer.getInt(i));
                list.add(row.getInt(i));
            }

            int i,j,flag,tmp;
            flag=1;
            // 降序排列
            for(i=1; i<list.size() && flag==1;i++ ){
                flag = 0; // 标记位，如果在一趟排序中，如果没有发生移动，就提前结束
                for (j=0;j<n-i;j++){
                    if(list.get(j) < list.get(j+1)){
                        flag=1; //  说明在 j,循环排序过程，有数据移动
                        tmp=list.get(j+1);
                        list.set(j+1,list.get(j));
                        list.set(j,tmp);
                    }
                }
            }

            //取出前三位数据
            for (int k = 0; k < buffer.size(); k++) {
                buffer.update(k,list.get(k));
            }

        }

        /**
         * 将缓冲区数据取出，拼接为字符串数据
         * @param buffer ： 分区合并结束之后，缓冲区数据
         * @return
         */
        public Object evaluate(Row buffer) {
            String values = null;
            values=buffer.getInt(0)+"";
            for (int i = 1; i < this.n; i++) {
                values=values+","+buffer.getInt(i);
            }
            return values;
        }
    }

}
