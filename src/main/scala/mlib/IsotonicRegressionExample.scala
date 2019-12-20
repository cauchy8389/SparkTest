package mlib

// $example on$
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.regression.IsotonicRegression
// $example off$
import org.apache.spark.sql.SparkSession

/**
  * An example demonstrating Isotonic Regression.
  * Run with
  * {{{
  * bin/run-example ml.IsotonicRegressionExample
  * }}}
  */
object IsotonicRegressionExample {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\hadoop-2.6.0-cdh5.8.5")
    val conf = new SparkConf().setMaster("local").setAppName("IsotonicRegression")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.setCheckpointDir("hdfs://192.168.1.51:9000/user/zhy/spark/wd_checkpoint")
    Logger.getRootLogger.setLevel(Level.WARN)

    // $example on$
    // Loads data.
    val dataset = spark.read.format("libsvm")
      .load("file:///F:/download/MLlib机器学习/数据/sample_isotonic_regression_libsvm_data.txt")

    // Trains an isotonic regression model.
    val ir = new IsotonicRegression()
    val model = ir.fit(dataset)

//    dataset.createOrReplaceTempView("isotonic");
//    spark.sql("select label from isotonic").show(20);
    dataset.select("label").show(20);
    println(s"dataset rows: ${dataset.collect().length} : ${dataset.count()}\n")
    println(s"Boundaries in increasing order: ${model.boundaries}\n")
    println(s"Boundaries in increasing order length: ${model.boundaries.size}\n")
    println(s"Predictions associated with the boundaries: ${model.predictions}\n")
    println(s"Predictions associated with the boundaries length: ${model.predictions.size}\n")

    // Makes predictions.
    model.transform(dataset).show(100)
    // $example off$

    spark.stop()
  }
}