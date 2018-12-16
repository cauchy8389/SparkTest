package SparkSQLScala

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by Administrator on 2018/9/9.
  */

case class Student(id:Int, name:String, age:Int)

object RDDToDataFrameReflection {
  //如果针对rdd执行sql操作
  //rdd 如何转化 dataFrame
  //反射形式
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\hadoop-2.6.0-cdh5.8.5")
    val conf  = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("create df")
    val sc = new SparkContext(conf)


    //需要获取一个  SQLContext
    val sqlContext = new  SQLContext(sc)
    //样例类  快速生成一个javaBean

    val sourceRDD = sc.textFile("D:\\text.txt")

    //由于scala 有着强大隐式转换功能
    //需要导入包
    //implicit Scala
    import sqlContext.implicits._

    //对象名 即列名
    val df = sourceRDD
      .map(line => line.split(","))
      .map(strs => Student(strs(0).trim.toInt, strs(1), strs(2).trim.toInt))
      .toDF()

    df.registerTempTable("Student")
    val sql = "select * from Student  where age <= 18"
    val selectDF =   sqlContext.sql(sql)

    selectDF
      .rdd
      .map(row => Student(row.getAs[Int]("id"), row.getAs[String]("name"), row.getAs[Int]("age")))
      .foreach(stu => println(stu.id + ":" + stu.name + ":" + stu.age))


    //spark  scala语法
    //针对spark 项目来说，开发某某平台。可能会涉及很多第三方的组件，
    //很多第三方组件并没有scala语法。所以企业开发平台都是用java语法
    //如果小任务级别，例如做一个简单统计，简单日志清洗，没有涉及到
    //第三方的组件，这个时候scala首选。
    //python
  }
}
