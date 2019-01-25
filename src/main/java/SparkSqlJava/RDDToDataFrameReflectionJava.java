package SparkSqlJava;

import mybean.Crime;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2018/9/9.
 */
public class RDDToDataFrameReflectionJava {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "E:\\hadoop-2.6.0-cdh5.8.5");
        SparkConf conf  = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("RDDToDataFrameReflectionJava");

        //SparkContext
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

        //sqlContext
        SQLContext sqlContext = new SQLContext(javaSparkContext);

        //rdd
        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile("F:\\CodingResource\\eshadoop-master\\Chapter7\\data\\crimes_dataset.csv");
        //针对普通的RDD不能直接用spark sql 需要做一步转化
        //把rdd转化成dataframe
        //首先 需要针对字符串 进行处理 即逗号分开
        //把每一行的String  解析之后  封装到Student类中。

        JavaRDD<Crime> map = stringJavaRDD.map(
                new Function<String, Crime>() {
                    @Override
                    public Crime call(String line) throws Exception {
                        CSVParser parser = CSVParser.parse(line, CSVFormat.RFC4180);
                        Crime c = new Crime();
                        CSVRecord record = parser.getRecords().get(0);
                        c.setId(record.get(0));
                        c.setCaseNumber(record.get(1));
                        c.setEventDate(record.get(2));
                        c.setBlock(record.get(3));
                        c.setIucr(record.get(4));
                        c.setPrimaryType(record.get(5));
                        c.setDescription(record.get(6));
                        c.setLocation(record.get(7));
                        c.setArrest(Boolean.parseBoolean(record.get(8)));
                        c.setDomestic(Boolean.parseBoolean(record.get(9)));
                        String lat = record.get(10);
                        String lon = record.get(11);

                        Map<String, Double> geoLocation = new HashMap<>();
                        geoLocation.put("lat", StringUtils.isEmpty(lat)?null:Double.parseDouble(lat));
                        geoLocation.put("lon", StringUtils.isEmpty(lon)?null:Double.parseDouble(lon));
                        c.setGeoLocation(geoLocation);
                        return c;
                    }
                }
        );

        //使用反射方式，将RDD -》DataFrame
        //确保  RDD中(map) 每个成员都是Student类型
        /*
        def createDataFrame(rdd : org.apache.spark.api.java.JavaRDD[_],
        beanClass : scala.Predef.Class[_]) : org.apache.spark.sql.DataFrame
         */
        Dataset<Row> dataFrame = sqlContext.createDataFrame(map, Crime.class);

        //得到一个df之后，使用sql   首先把这个df注册成一张临时表
        dataFrame.createOrReplaceTempView("crime");
        //select * from 表名
        String sql = "select * from crime where eventDate <= 1438522200000";

        //注意 查询的结果 返回 仍然是一个dataFrame
        Dataset<Row> sql1 = sqlContext.sql(sql);

        //show  只显示10行
        //sql1.show();
        //一般在实际环境中，不会用到show仅仅输出到终端。
        //而是把这个结果保存起来。
        //df列式存储，首先把df --> rdd
        JavaRDD<Row> rowJavaRDD = sql1.javaRDD();

        //通过 df转化得到的rdd ，即javaRDD()。这个时候rdd中的存储每一行
        //都封装到了row类中。
        JavaRDD<Crime> map1 = rowJavaRDD.map(
                new Function<Row, Crime>() {
                    @Override
                    public Crime call(Row v1) throws Exception {
                        Crime cri = new Crime();
                        //申请大量对象，会增加gc 垃圾回收器负担
                        //row 每一行的成员获取的时候，两种形式
                        //一种是通过 位置来获取  一般不采用这种形式
                        //因为 通过rdd 转化成df之后，优化之后，顺序就变了
//                        stu.setAge(v1.getInt(0));
//                        stu.setId(v1.getInt(1));
//                        stu.setName(v1.getString(2));

                        //推荐 通过列名来获取  推荐使用这种方式
                        cri.setId(v1.getAs("id"));
                        cri.setCaseNumber(v1.getAs("caseNumber"));
                        Long eventDate  = v1.getAs("eventDate");
                        cri.setEventDate(eventDate);

                        return cri;
                    }
                }
        );

        //后面是自定义的操作
        List<Crime> collect = map1.collect();

        for(Crime temp: collect) {
            System.out.println(temp);
        }


    }
}
