package mlib

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


object als2 {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\hadoop-2.6.0-cdh5.8.5")
    val conf = new SparkConf().setMaster("local").setAppName("ALSExample")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.setCheckpointDir("hdfs://zhy.cauchy8389.com:9000/user/zhy/spark/wd_checkpoint")

    import spark.implicits._
    val ratings = spark.read.textFile("file:///E:/download/MLlib机器学习/数据/sample_itemcf2.txt")
      .map(_.split(',') match {
        case Array(user, item, rate) =>
          Rating(user.toInt, item.toInt, rate.toDouble)
      })
      .toDF()

    var modelPath = "hdfs://zhy.cauchy8389.com:9000/user/zhy/spark/mllib";
    var hadoopConf = new Configuration()
    var fs = FileSystem.get(hadoopConf)
    var dstPath = new Path(modelPath);

    val model = fs.exists(dstPath) match {
      case true => ALSModel.load(modelPath)
      case _ => {
        val als = new ALS()
        als.setRank(10)
          .setMaxIter(5)
          .setRegParam(0.01)
          .setUserCol("user")
          .setItemCol("product")
          .setRatingCol("rating")
        als.fit(ratings)
      }
    }

    val userRecs:DataFrame = model.recommendForAllUsers(10)
    userRecs.foreach(row => {
        println(row(0) + ":")
        println(row(1))

      }
    )

    val userArray:Array[Row] = userRecs.collect()
    userArray.foreach(row => {
      println(row.get(0) + ":")
      val arrayPredict : Seq[Row] = row.getSeq(1)
      arrayPredict.foreach(rowPredict =>{
        println(rowPredict(0) + "@" + rowPredict(1))
//        println(rowPredict.head.asInstanceOf[Row](0) + "@"
//          + rowPredict.head.asInstanceOf[Row](1))
      })

    })

    var ss : Option[Int] = Some(1)
    //    val ss = SparkSession.builder.config(conf).getOrCreate
    //    val parquetFile = ss.read.parquet(modelPath + "/data/product/part-00000-e8c3333d-ce82-4cdb-b56a-77e0f39fd854-c000.snappy.parquet")
    //    parquetFile.take(100).foreach(println);

    spark.stop()
  }
}
