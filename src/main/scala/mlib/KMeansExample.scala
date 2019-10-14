package mlib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.evaluation.ClusteringEvaluator
// $example off$
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.clustering.KMeans

object KMeansExample {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "E:\\hadoop-2.6.0-cdh5.8.5")
    val conf = new SparkConf().setMaster("local").setAppName("KMeansExample")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.setCheckpointDir("hdfs://192.168.1.51:9000/user/zhy/spark/wd_checkpoint")
    Logger.getRootLogger.setLevel(Level.WARN)

    // $example on$
    // Loads data.
    val dataset = spark.read.format("libsvm").load("file:///F:/download/MLlib机器学习/数据/sample_kmeans_data.txt")

    // Trains a k-means model.
    val kmeans = new KMeans().setK(2).setSeed(1L)
    val model = kmeans.fit(dataset)

    // Make predictions
    val predictions = model.transform(dataset)

    // Evaluate clustering by computing Silhouette score
    val evaluator = new ClusteringEvaluator()

    val silhouette = evaluator.evaluate(predictions)
    println(s"Silhouette with squared euclidean distance = $silhouette")
    predictions.foreach(println(_));

    // Shows the result.
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)

    //model.
    // $example off$

    spark.stop()
  }
}
