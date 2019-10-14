package mlib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.SparkSession

object LinearRegression {

  def main(args: Array[String]) {
    // 构建Spark对象
    System.setProperty("hadoop.home.dir", "E:\\hadoop-2.6.0-cdh5.8.5")
    val conf = new SparkConf().setMaster("local").setAppName("LinearRegression")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.setCheckpointDir("hdfs://zhy.cauchy8389.com:9000/user/zhy/spark/wd_checkpoint")
    Logger.getRootLogger.setLevel(Level.WARN)

    //读取样本数据
    val data_path1 = "file:///E:/download/MLlib机器学习/数据/lpsa.data"

    import spark.implicits._
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

    val dataRow = examples.toDF("Col1", "Col2", "Col3", "Col4", "Col5", "Col6", "Col7", "Col8", "Col9")
    val colArray = Array("Col2", "Col3", "Col4", "Col5", "Col6", "Col7", "Col8", "Col9")
    val assembler = new VectorAssembler().setInputCols(colArray).setOutputCol("features")
    val vecDF: DataFrame = assembler.transform(dataRow)

    // 新建线性回归模型，并设置训练参数
    val numIterations = 100
//    val stepSize = 1
//    val miniBatchFraction = 1.0
//    val model = LinearRegressionWithSGD.train(examples, numIterations, stepSize, miniBatchFraction)
    val lr1 = new LinearRegression()
    val lr2 = lr1.setFeaturesCol("features").setLabelCol("Col1").setFitIntercept(true)
    // RegParam：正则化
    val lr3 = lr2.setMaxIter(numIterations).setRegParam(0.3).setElasticNetParam(0.8)
    val lr = lr3

    // 将训练集合代入模型进行训练
    val lrModel = lr.fit(vecDF)


    val data_predict = spark.read.textFile(data_path1)
    val examples_predict = data_predict.map { line =>
      val parts = line.split(',')
      // LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
      val _2parts = parts(1).split(' ')
      (parts(0).toDouble,_2parts(0).toDouble,_2parts(1).toDouble,_2parts(2).toDouble,
        _2parts(3).toDouble,_2parts(4).toDouble,_2parts(5).toDouble,_2parts(6).toDouble,
        _2parts(7).toDouble)
    }.cache()
    val dataRow_predict = examples_predict.toDF("Col1", "Col2", "Col3", "Col4", "Col5", "Col6", "Col7", "Col8", "Col9")
    val colArray_predict = Array("Col2", "Col3", "Col4", "Col5", "Col6", "Col7", "Col8", "Col9")
    val assembler_predict = new VectorAssembler().setInputCols(colArray_predict).setOutputCol("features")
    val vecDF_predict: DataFrame = assembler_predict.transform(dataRow_predict)

    // lrModel.weights
    println(s"WeightCol:${lrModel.weightCol} Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // Summarize the model over the training set and print out some metrics
    val trainingSummary = lrModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
    trainingSummary.residuals.show()  //残差
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")  // r2值越接近1越好
    // $example off$
    // https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/ml/LinearRegressionExample.scala

    val predictions: DataFrame = lrModel.transform(vecDF_predict)
    println("预测输出预测结果")
    val predict_result: DataFrame = predictions.selectExpr("features","Col1", "round(prediction,10) as prediction")
    predict_result.foreach(println(_))

    spark.stop()

  }

}
