package mlib.featureext

import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object StandardScalarSample {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "E:\\hadoop-2.6.0-cdh5.8.5")
    val conf = new SparkConf().setMaster("local").setAppName("StandardScalerExample")

    val spark = SparkSession
      .builder.config(conf)
      .getOrCreate()

    // $example on$
    val dataFrame = spark.read.format("libsvm").load("file:///F:/download/MLlib机器学习/数据/sample_libsvm_data.txt")

    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(false)

    // Compute summary statistics by fitting the StandardScaler.
    val scalerModel = scaler.fit(dataFrame)

    // Normalize each feature to have unit standard deviation.
    val scaledData = scalerModel.transform(dataFrame)
    scaledData.show(false)
    // $example off$

    spark.stop()
  }
}