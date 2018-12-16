package SparkSQLScala

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/9/9.
  */
object HiveDataSourceScala {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "E:\\hadoop-2.6.0-cdh5.8.5")
    val conf  = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("create df")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    //这个sql语句 执行的hive的hsql
    val  df =  hiveContext.sql("select * from hadoop.human")
    //df.show()

    //注册临时表
    df.registerTempTable("student")
    hiveContext.sql("select * from student where age <= 19")

  }
}
