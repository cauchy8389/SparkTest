package mlib

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{ Level, Logger }


case class Rating (user: Int,
                    product: Int,
                    rating: Double)

object als1 {
  def parseRating(str: String): Rating = {
    val fields = str.split(",")
    //assert(fields.size == 4)
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
  }

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "E:\\hadoop-2.6.0-cdh5.8.5")

    //0 构建Spark对象
    val conf = new SparkConf().setMaster("local").setAppName("ALSExample")
    //  .config("spark.sql.warehouse.dir","E:/ideaWorkspace/ScalaSparkMl/spark-warehouse")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.setCheckpointDir("hdfs://zhy.cauchy8389.com:9000/user/zhy/spark/wd_checkpoint")
    Logger.getRootLogger.setLevel(Level.WARN)

    //1 读取样本数据
    import spark.implicits._
    val ratings = spark.read.textFile("file:///E:/download/MLlib机器学习/数据/test.data")
      .map(_.split(',') match {
        case Array(user, item, rate) =>
          Rating(user.toInt, item.toInt, rate.toDouble)
      })
      .toDF()

    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

//    val data = sc.textFile("E:\\download\\MLlib机器学习\\数据\\test.data")
//    val ratings = data.map(parseRating)

    //2 建立模型
    //val rank = 10
    //val numIterations = 20
    //val model = ALS.train(ratings, rank, numIterations, 0.01)

    // Build the recommendation model using ALS on the training data
    //使用训练数据训练模型
    //这里的ALS是import org.apache.spark.ml.recommendation.ALS，不是mllib中的哈
    //setMaxiter设置最大迭代次数
    //setRegParam设置正则化参数，日lambda这个不是更明显么
    //setUserCol设置用户id列名
    //setItemCol设置物品列名
    //setRatingCol设置打分列名
    val als = new ALS()
    als.setRank(10)
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("user")
      .setItemCol("product")
      .setRatingCol("rating")


    //fit给输出的数据，训练模型，fit返回的是ALSModel类
    var modelPath = "hdfs://zhy.cauchy8389.com:9000/user/zhy/spark/mllib";
    var hadoopConf = new Configuration()
    var fs = FileSystem.get(hadoopConf)
    var dstPath = new Path(modelPath);

    val model = fs.exists(dstPath) match {
      case true => ALSModel.load(modelPath)
      case _ => als.fit(training)
    }

    // Evaluate the model by computing the RMSE on the test data
    // Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
    model.setColdStartStrategy("drop")
    val predictions = model.transform(test)

    //RegressionEvaluator这个类是用户评估预测效果的，预测值与原始值
    //这个setLabelCol要和als设置的setRatingCol一致，不然会报错哈
    //RegressionEvaluator的setPredictionCol必须是prediction因为，ALSModel的默认predictionCol也是prediction
    //如果要修改的话必须把ALSModel和RegressionEvaluator一起修改
    //model.setPredictionCol("prediction")和evaluator.setPredictionCol("prediction")
    //setMetricName这个方法，评估方法的名字，一共有哪些呢？
    //rmse-平均误差平方和开根号
    //mse-平均误差平方和
    //mae-平均距离（绝对）
    //r2-没用过不知道
    //这里建议就是用rmse就好了，其他的基本都没用，当然还是要看应用场景，这里是预测分值就是用rmse。如果是预测距离什么的mae就不从，看场景哈
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")

    val rmse = evaluator.evaluate(predictions)
    println("Root-mean-square error = "+rmse)
    //0.016603969187081208
    //0.028604507446289062


    // Generate top 10 movie recommendations for each user
    val userRecs = model.recommendForAllUsers(10)
    // Generate top 10 user recommendations for each movie
    val productRecs = model.recommendForAllItems(10)

    // Generate top 10 movie recommendations for a specified set of users
    val users = ratings.select(als.getUserCol).distinct().limit(3)
    val userSubsetRecs = model.recommendForUserSubset(users, 10)
    // Generate top 10 user recommendations for a specified set of movies
    val movies = ratings.select(als.getItemCol).distinct().limit(3)
    val movieSubSetRecs = model.recommendForItemSubset(movies, 10)


    if(!fs.exists(dstPath)) {
      model.save(modelPath)
    }



    //3 预测结果
//    val usersProducts = ratings.map {
//      case Rating(user, product, rate) =>
//        (user, product)
//    }
//    val predictions =
//      model.predict(usersProducts).map {
//        case Rating(user, product, rate) =>
//          ((user, product), rate)
//      }
//    val ratesAndPreds = ratings.map {
//      case Rating(user, product, rate) =>
//        ((user, product), rate)
//    }.join(predictions)
//    val MSE = ratesAndPreds.map {
//      case ((user, product), (r1, r2)) =>
//        val err = (r1 - r2)
//        err * err
//    }.mean()
//    println("Mean Squared Error = " + MSE)
//
//    //4 保存/加载模型
//    var modelPath = "hdfs://zhy.cauchy8389.com:9000/user/zhy/spark/mllib";
//    model.save(sc, modelPath)
//    val sameModel = MatrixFactorizationModel.load(sc, modelPath)

    spark.stop()
  }

}
