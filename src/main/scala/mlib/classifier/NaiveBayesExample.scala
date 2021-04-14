package mlib.classifier

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.sql.SparkSession

object NaiveBayesExample {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\hadoop-2.6.0-cdh5.8.5")
    val conf = new SparkConf().setMaster("local").setAppName("NaiveBayes")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.setCheckpointDir("hdfs://192.168.1.51:9000/user/zhy/spark/wd_checkpoint")
    Logger.getRootLogger.setLevel(Level.WARN)

    // $example on$
    // Load the data stored in LIBSVM format as a DataFrame.
    val data = spark.read.format("libsvm").load("file:///F:/download/MLlib机器学习/数据/sample_libsvm_naive_bayes.txt")

    val indexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexed")
      .setMaxCategories(6)

    // Split the data into training and test sets (30% held out for testing)
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3), seed = 1234L)

    // Train a NaiveBayes model.
    val naiveBayes = new NaiveBayes()
    //.fit(trainingData)

    val pipeline = new Pipeline()
      .setStages(Array(indexer, naiveBayes))

    val model = pipeline.fit(trainingData)

    println("--------original data----------")
    data.show()
    println("-------test data-----")
    testData.show()
    // Select example rows to display.
    val predictions = model.transform(testData)
    predictions.show()

    println("rawPrediction:")
    predictions.select("rawPrediction").foreach(row => {
      println(row)
    })

    println("features:")
    predictions.select("features").foreach(row => {
      println(row)
    })

    println("probability:")
    predictions.select("probability").foreach(row => {
      println(row)
    })

    // Select (prediction, true label) and compute test error
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println(s"Test set accuracy = $accuracy")
    // $example off$

    spark.stop()
  }
}
