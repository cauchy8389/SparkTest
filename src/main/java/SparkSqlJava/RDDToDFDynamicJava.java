package SparkSqlJava;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * 以编程方式动态指定元数据，将RDD转化成DataFrame
 * 这个时候 就不依赖于 javaBean
 * Created by Administrator on 2018/9/9.
 */
public class RDDToDFDynamicJava {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "E:\\hadoop-2.6.0-cdh5.8.5");
        SparkConf conf  = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("RDDToDataFrameReflectionJava");

        //SparkContext
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

        //sqlContext
        SQLContext sqlContext = new SQLContext(javaSparkContext);

        //第一步，创建一个RDD
        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile("D:\\text.txt");
        /*
        def createDataFrame(rowRDD : org.apache.spark.api.java.JavaRDD[org.apache.spark.sql.Row],
            schema : org.apache.spark.sql.types.StructType) : org.apache.spark.sql.DataFrame
            第一个参数 是row类型的RDD       源数据  根据列区分
            第二个参数  是用于描述类型和名称      指定每一列的类型 和名称
        */
        //第二步，需要把rdd中string类型转化成row类型
        JavaRDD<Row> map = stringJavaRDD.map(
                new Function<String, Row>() {
                    @Override
                    public Row call(String v1) throws Exception {
                        String[] split = v1.split(",");
                        return RowFactory.create(
                                Integer.valueOf(split[0]),
                                split[1],
                                Integer.valueOf(split[2])
                        );
                    }
                }
        );

        //动态构造元数据
        //可以通过 一些配置文件  来获取


        ArrayList<StructField> structFields = new ArrayList<>();

        structFields.add(DataTypes.createStructField("id",
                DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("name",
                DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("age",
                DataTypes.IntegerType, true));

        //用于描述 每一列中名称，和类型，以及是否为空
        StructType st = DataTypes.createStructType(structFields);
        Dataset<Row> dataFrame = sqlContext.createDataFrame(map, st);

        dataFrame.registerTempTable("student");
        String sql = "select * from student where age > 19";
        Dataset<Row> sql1 = sqlContext.sql(sql);


        List<Row> collect = sql1.javaRDD().collect();

        for(Row row:collect) {
            System.out.println(row);
        }


    }
}
