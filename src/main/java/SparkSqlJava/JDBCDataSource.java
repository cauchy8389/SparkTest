package SparkSqlJava;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
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
        options.put("url", "jdbc:mysql://10.83.8.120:3306/crm");
        options.put("dbtable", "ec_claim_2");
        options.put("user", "cauchy8389");
        options.put("password", "tn_cauchy8389");
        Dataset<Row> jdbc =
                sqlContext.read().format("jdbc").options(options).load();

        jdbc.createOrReplaceTempView("ec");

        //一般企业中 这种从jdbc 中读取到dataframe  用法比较少用
        //需要用到例如 MySQL数据，直接在mysql去查询
        Dataset<Row> sql = sqlContext.sql("select * from ec where Id < 500");

        sql.show();

    }
}
