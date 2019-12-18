package mlib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.max

object LogisticRegressionSummaryExample {

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "E:\\hadoop-2.6.0-cdh5.8.5")
    val conf = new SparkConf().setMaster("local").setAppName("LogisticRegressionSummary")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.setCheckpointDir("hdfs://192.168.1.51:9000/user/zhy/spark/wd_checkpoint")
    Logger.getRootLogger.setLevel(Level.WARN)

    //读取样本数据
    val data_path1 = "file:///F:/download/MLlib机器学习/数据/sample_libsvm_data.txt"

    import spark.implicits._
    val training = spark.read.format("libsvm").load(data_path1)
    //

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setLabelCol("label")
      .setElasticNetParam(0.8)

    // Fit the model
    val lrModel = lr.fit(training)

    // Print the coefficients and intercept for logistic regression
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // $example on$
    // Extract the summary from the returned LogisticRegressionModel instance trained in the earlier
    // example
    val trainingSummary = lrModel.binarySummary

    // Obtain the objective per iteration.
    val objectiveHistory = trainingSummary.objectiveHistory
    println("objectiveHistory:")
    objectiveHistory.foreach(loss => println(loss))

    // Obtain the receiver-operating characteristic as a dataframe and areaUnderROC.
    val roc = trainingSummary.roc
    roc.show(100)
    println(s"areaUnderROC: ${trainingSummary.areaUnderROC}")

    // Set the model threshold to maximize F-Measure
    val fMeasure = trainingSummary.fMeasureByThreshold
    val maxFMeasure = fMeasure.select(max("F-Measure")).head().getDouble(0)
    println(s"maxFMeasure : ${maxFMeasure}")
    val bestThreshold = fMeasure.where($"F-Measure" === maxFMeasure)
      .select("threshold").head().getDouble(0)
    lrModel.setThreshold(bestThreshold)
    // $example off$

    //println(lrModel.predict(Vectors.sparse(692, Seq((1,15.0), (55,245.0)))))
    println(lrModel.predict(Vectors.sparse(692, Seq((129,159.0),(130,142.0),(156,11.0),(157,220.0),(158,141.0),(184,78.0),(185,254.0),(186,141.0),(212,111.0),(213,254.0),(214,109.0),(240,196.0),(241,221.0),(242,15.0),(267,26.0),(268,221.0),(269,159.0),(295,63.0),(296,254.0),(297,159.0),(323,178.0),(324,254.0),(325,93.0),(350,7.0),(351,191.0),(352,254.0),(353,97.0),(378,42.0),(379,255.0),(380,254.0),(381,41.0),(406,42.0),(407,254.0),(408,195.0),(409,10.0),(434,141.0),(435,255.0),(436,78.0),(461,11.0),(462,202.0),(463,254.0),(464,59.0),(489,86.0),(490,254.0),(491,254.0),(492,59.0),(517,142.0),(518,254.0),(519,248.0),(520,52.0),(545,142.0),(546,254.0),(547,195.0),(573,142.0),(574,254.0),(575,164.0),(601,142.0),(602,254.0),(603,77.0),(629,142.0),(630,254.0),(631,131.0),(657,77.0),(658,172.0),(659,5.0)))))


    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val ds = lrModel.transform(training)
    val rmse = evaluator.evaluate(ds)
    println(s"Root Mean Squared Error (RMSE) on test data = $rmse")

    spark.stop()
  }
}
