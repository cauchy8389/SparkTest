package mlib.classifier

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.mllib.evaluation.{MulticlassMetrics, RegressionMetrics}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object GBTExample {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\hadoop-2.6.0-cdh5.8.5")

    //1 构建Spark对象
    val conf = new SparkConf().setMaster("local").setAppName("GradientBoostedTreeExample")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.setCheckpointDir("hdfs://192.168.1.51:9000/user/zhy/spark/wd_checkpoint")
    Logger.getRootLogger.setLevel(Level.WARN)


    val dataFrame = spark.read.format("libsvm").load("file:///F:/download/MLlib机器学习/数据/sample_libsvm_data.txt")


    // Set up Pipeline
    val stages = new mutable.ArrayBuffer[PipelineStage]()

//    val labelIndexer = new StringIndexer()
//      .setInputCol("label")
//      .setOutputCol("indexedLabel")
//    stages += labelIndexer

    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(dataFrame)

    val Array(training, test) = dataFrame.randomSplit(Array(0.9, 0.1), seed = 12345)

    stages += featureIndexer

    //val vectorAssembler = new VectorAssembler().setInputCols(dataFrame.columns).setOutputCol("features_out")

    val gbt = new GBTClassifier()
      //.setFeaturesCol(vectorAssembler.getOutputCol)
      .setFeaturesCol("indexedFeatures")
      .setLabelCol("label")
      .setMaxIter(10)

    //stages += vectorAssembler
    stages += gbt
    val pipeline = new Pipeline().setStages(stages.toArray)

    // Fit the Pipeline
    val startTime = System.nanoTime()
    val model = pipeline.fit(training)
    //val model = pipeline.fit(dataFrame)
    val elapsedTime = (System.nanoTime() - startTime) / 1e9
    println(s"Training time: $elapsedTime seconds")

    val holdout = model.transform(test).select("prediction","label")
    //val holdout = model.transform(dataFrame).select("prediction","label")
    holdout.show()

    // have to do a type conversion for RegressionMetrics
    val rm = new RegressionMetrics(holdout.rdd.map(x => (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double])))

    println("Test Metrics")
    println("Test Explained Variance:")
    println(rm.explainedVariance)
    println("Test R^2 Coef:")
    println(rm.r2)
    println("Test MSE:")
    println(rm.meanSquaredError)
    println("Test RMSE:")
    println(rm.rootMeanSquaredError)

    val predictions = model.transform(test).select("prediction").rdd.map(_.getDouble(0))
    val labels = model.transform(test).select("label").rdd.map(_.getDouble(0))
    val accuracy = new MulticlassMetrics(predictions.zip(labels)).accuracy
    println(s"  Accuracy : $accuracy")

//    holdout.rdd.map(x => x(0).asInstanceOf[Double]).repartition(1).saveAsTextFile("/home/ubuntu/work/ml-resources/spark-ml/results/GBT.xls")

  }

}
