package SparkSqlJava;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.util.HashMap;

/**
 * Created by Administrator on 2018/9/9.
 */
public class JDBCDataSource {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "E:\\hadoop-2.6.0-cdh5.8.5");
        SparkConf conf  = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("Hive Data Source");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        //可以从mysql中获取数据
        //从jdbc中获取数据 mysql
        //需要指明 url   databases  table
        HashMap<String, String> options = new HashMap<>();
        options.put("url", "jdbc:mysql://candle.hwua.com:3306/sqoop");
        options.put("dbtable", "human2");
        options.put("user", "hive");
        options.put("password", "hive");
        DataFrame jdbc =
                sqlContext.read().format("jdbc").options(options).load();

        jdbc.registerTempTable("student");

        //一般企业中 这种从jdbc 中读取到dataframe  用法比较少用
        //需要用到例如 MySQL数据，直接在mysql去查询
        DataFrame sql = sqlContext.sql("select * from student where age >= 20");

        sql.show();

    }
}
