package SparkSQLScala

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/9/9.
  */
object MySparkSql {
  def main(args: Array[String]): Unit = {
    //spark sql  就是提供一个针对rdd 执行sql语句组件。
    //spark   shark  ，shark底层很多东西依赖于hive
    //spark  sql  完全摒弃了 hive。

    /*
    spark sql 特点：
    1.支持多种数据源：  hive  rdd， parquet ,json jdbc...
    2.多种性能优化技术
    3.组件扩展性：UDF ，用户可以自己重新开发，动态扩展

    sparkContext RDD驱动
    DataFrame 数据集，以列的形式组织的，分布式的数据集合。
    和HBase类似

    针对 RDD  进行SQL语句
    RDD --> DataFrame 那么就可以执行sql语句
     */
    //创建一个spark sql
    System.setProperty("hadoop.home.dir", "E:\\hadoop-2.6.0-cdh5.8.5")
    val conf  = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("create df")
    val sc = new SparkContext(conf)


    //需要获取一个  SQLContext
    val sqlContext = new  SQLContext(sc)
    //df DataFrame   从本地去读取一个json文本
    //val df = sqlContext.read.json("E:\\student.json")
    //sqlContext

    //select * from ....
    //查询的结果  和json文本的顺序可能不一致
    //df.show()

    //从hdfs中读取
    val df = sqlContext.read.json("hdfs://candle.hwua.com:9000/user/candle/spark/student.json")
    df.show()

    //spark sql  如果执行sql语句
    //一种方式 使用df提供一系列的函数，比较直观。一般用于没有注册临时表的df
    //desc  .....
    df.printSchema()

    //select name form .....
    df.select("name").show()

    df.select(df("name"), df("age")+1).show()
    df.filter(df("age") > 18).show()
    //另外一种形式，把这个df注册成一张表。注意：spark 2.0 支持sql标准。
  }
}
