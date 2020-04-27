package mlib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.sql.SparkSession

object FPGrowth {
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession
    System.setProperty("hadoop.home.dir", "E:\\hadoop-2.6.0-cdh5.8.5")

    //1 构建Spark对象
    val conf = new SparkConf().setMaster("local").setAppName("FPGrowth")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.setCheckpointDir("hdfs://192.168.1.51:9000/user/zhy/spark/wd_checkpoint")
    Logger.getRootLogger.setLevel(Level.WARN)

    import spark.implicits._

    // $example on$
    val dataset = spark.createDataset(Seq(
      "1 2 5",
      "1 2 3 5",
      "1 2")
    ).map(t => t.split(" ")).toDF("items")

    val fpgrowth = new FPGrowth().setItemsCol("items").setMinSupport(0.5).setMinConfidence(0.6)
    val model = fpgrowth.fit(dataset)

    // Display frequent itemsets.
    model.freqItemsets.show()

    // Display generated association rules.
    model.associationRules.show()

    // transform examines the input items against all the association rules and summarize the
    // consequents as prediction
    model.transform(dataset).show()
    // $example off$

    spark.stop()
  }

}
