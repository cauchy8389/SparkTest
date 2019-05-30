package SparkSQLScala


import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/9/9.
  */
object RDDToDFDynamicScala {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\hadoop-2.6.0-cdh5.8.5")
    val conf  = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("create df")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)


    val rowRDD = sc.textFile("D:\\text.txt")
      .map(line => Row(line.split(",")(0).toInt, line.split(",")(1), line.split(",")(2).toInt) )

    val  st =  StructType(Array(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true)
    ))

    val DF = sqlContext.createDataFrame(rowRDD ,st)

    DF.createOrReplaceTempView("Student")

    val sql = "select * from Student"
    val DF2 = sqlContext.sql(sql)

    DF2.rdd.foreach(println)
  }
}
