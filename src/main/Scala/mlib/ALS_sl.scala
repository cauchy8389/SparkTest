package mlib

import org.apache.spark.SparkConf
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path


object als2 {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\hadoop-2.6.0-cdh5.8.5")
    val conf = new SparkConf().setMaster("local").setAppName("ALSExample")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    var modelPath = "hdfs://zhy.cauchy8389.com:9000/user/zhy/spark/mllib";
    var hadoopConf = new Configuration()
    var fs = FileSystem.get(hadoopConf)
    var dstPath = new Path(modelPath);

    if(fs.exists(dstPath)){
      val sameModel = ALSModel.load(modelPath)
    }

    var ss = 1
    //    val ss = SparkSession.builder.config(conf).getOrCreate
    //    val parquetFile = ss.read.parquet(modelPath + "/data/product/part-00000-e8c3333d-ce82-4cdb-b56a-77e0f39fd854-c000.snappy.parquet")
    //    parquetFile.take(100).foreach(println);

    spark.stop()
  }
}
