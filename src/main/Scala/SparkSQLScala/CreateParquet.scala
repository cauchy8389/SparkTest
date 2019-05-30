package SparkSQLScala

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2018/9/9.
  */
object CreateParquet {
  def main(args: Array[String]): Unit = {
    //读取json文本 保存成一个parquet格式

    System.setProperty("hadoop.home.dir", "E:\\hadoop-2.6.0-cdh5.8.5")
    val conf  = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("create df")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val df = spark.read.parquet("E:\\person.parquet");

    df.show()

    //把df保存成  parquet类型
    //person.parquet  压缩格式  load  save
    //df.write.parquet("E:\\parquet")
    df.write.json("E:\\myJson")
    //df.show()
  }
}
