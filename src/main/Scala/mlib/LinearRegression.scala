package mlib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.sql.SparkSession

object LinearRegression {

  def main(args: Array[String]) {
    // 构建Spark对象
    System.setProperty("hadoop.home.dir", "E:\\hadoop-2.6.0-cdh5.8.5")
    val conf = new SparkConf().setMaster("local").setAppName("LinearRegressionWithSGD")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.setCheckpointDir("hdfs://zhy.cauchy8389.com:9000/user/zhy/spark/wd_checkpoint")
    Logger.getRootLogger.setLevel(Level.WARN)

    //读取样本数据
    val data_path1 = "file:///E:/download/MLlib机器学习/数据/lpsa.data"
    val data = spark.read.textFile(data_path1)
    val examples = data.map { line =>
      val parts = line.split(',')
      // LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
      val _2parts = parts(1).split(' ')
      (parts(0).toDouble,_2parts(0).toDouble,_2parts(1).toDouble,_2parts(2).toDouble,
        _2parts(3).toDouble,_2parts(4).toDouble,_2parts(5).toDouble,_2parts(6).toDouble,
        _2parts(7).toDouble)
    }.cache()
    val numExamples = examples.count()

    // 新建线性回归模型，并设置训练参数
    val numIterations = 100
//    val stepSize = 1
//    val miniBatchFraction = 1.0
//    val model = LinearRegressionWithSGD.train(examples, numIterations, stepSize, miniBatchFraction)
    val lr1 = new LinearRegression()
    val lr2 = lr1.setFeaturesCol("features").setLabelCol("Murder").setFitIntercept(true)
    // RegParam：正则化
    val lr3 = lr2.setMaxIter(numIterations).setRegParam(0.3).setElasticNetParam(0.8)
    val lr = lr3

    // 将训练集合代入模型进行训练
    val lrModel = lr.fit(examples)

    // lrModel.weights
    lrModel.weightCol
    lrModel.intercept

    // 对样本进行测试
    val prediction = lrModel.predict(examples.map(_.features))
    val predictionAndLabel = prediction.zip(examples.map(_.label))
    val print_predict = predictionAndLabel.take(20)
    println("prediction" + "\t" + "label")
    for (i <- 0 to print_predict.length - 1) {
      println(print_predict(i)._1 + "\t" + print_predict(i)._2)
    }
    // 计算测试误差
    val loss = predictionAndLabel.map {
      case (p, l) =>
        val err = p - l
        err * err
    }.reduce(_ + _)
    val rmse = math.sqrt(loss / numExamples)
    println(s"Test RMSE = $rmse.")

    // 模型保存
    val ModelPath = "/user/huangmeiling/LinearRegressionModel"
    model.save(sc, ModelPath)
    val sameModel = LinearRegressionModel.load(sc, ModelPath)

  }

}
