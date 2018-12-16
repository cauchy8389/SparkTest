package SparkSqlJava;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Created by Administrator on 2018/9/9.
 */
public class HiveDataSourceJava {
    public static void main(String[] args) {
        //从hive中获取数据 到 dataFrame中
        System.setProperty("hadoop.home.dir", "E:\\hadoop-2.6.0-cdh5.8.5");
        SparkConf conf  = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("Hive Data Source");

        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        //注意事项：  其他的输入源驱动器 都是 SQLContext
        //hive中的驱动器  HiveContext
        // hql语句     sql语句
        //spark 2.0  无论是哪种输入源 都是  SQLContext
        //把hive中的配置  hive-site.xml文件拷贝到 工程中
        //因为 spark 连接到哪个 hive  需要指明
        HiveContext hiveContext = new HiveContext(javaSparkContext);

        //使用HiveContext  可以直接使用HSQL语句
        //通过查询hive表，把查询的结果  保存到了 DataFrame中
        //hive  hsql 语句
        DataFrame sql = hiveContext.sql("select * from hadoop.human");


        sql.registerTempTable("student");
        //把表名 注册成了 student   如果查询这个注册的表名
        //那么 这条语句是spark sql(dataFrame)中 sql语句
        DataFrame sql1 = hiveContext.sql("select * from student where age >= 19");

        sql1.show();

    }
}
