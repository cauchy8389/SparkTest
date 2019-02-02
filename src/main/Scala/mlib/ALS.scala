package mlib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object als1 {

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "E:\\hadoop-2.6.0-cdh5.8.5")

    //0 构建Spark对象
    val conf = new SparkConf().setMaster("local").setAppName("ALS")
    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)
    sc.setCheckpointDir("hdfs://zhy.cauchy8389.com:9000/user/zhy/spark/wd_checkpoint")

    //1 读取样本数据
    val data = sc.textFile("E:\\download\\MLlib机器学习\\数据\\test.data")
    val ratings = data.map(_.split(',') match {
      case Array(user, item, rate) =>
        Rating(user.toInt, item.toInt, rate.toDouble)
    })

    //2 建立模型
    val rank = 10
    val numIterations = 20
    val model = ALS.train(ratings, rank, numIterations, 0.01)

    //3 预测结果
    val usersProducts = ratings.map {
      case Rating(user, product, rate) =>
        (user, product)
    }
    val predictions =
      model.predict(usersProducts).map {
        case Rating(user, product, rate) =>
          ((user, product), rate)
      }
    val ratesAndPreds = ratings.map {
      case Rating(user, product, rate) =>
        ((user, product), rate)
    }.join(predictions)
    val MSE = ratesAndPreds.map {
      case ((user, product), (r1, r2)) =>
        val err = (r1 - r2)
        err * err
    }.mean()
    println("Mean Squared Error = " + MSE)

    //4 保存/加载模型
    var modelPath = "hdfs://zhy.cauchy8389.com:9000/user/zhy/spark/mllib";
    model.save(sc, modelPath)
    val sameModel = MatrixFactorizationModel.load(sc, modelPath)

//    val ss = SparkSession.builder.config(conf).getOrCreate
//    val parquetFile = ss.read.parquet(modelPath + "/data/product/part-00000-e8c3333d-ce82-4cdb-b56a-77e0f39fd854-c000.snappy.parquet")
//    parquetFile.take(100).foreach(println);

  }

}
