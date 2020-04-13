package mlib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.sql.SparkSession

object LDAExample {
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession
    System.setProperty("hadoop.home.dir", "E:\\hadoop-2.6.0-cdh5.8.5")

    //1 构建Spark对象
    val conf = new SparkConf().setMaster("local").setAppName("LDAExample")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.setCheckpointDir("hdfs://192.168.1.51:9000/user/zhy/spark/wd_checkpoint")
    Logger.getRootLogger.setLevel(Level.WARN)

    // $example on$
    // Loads data.
    val dataset = spark.read.format("libsvm")
      .load("file:///F:/download/MLlib机器学习/数据/sample_lda_libsvm_data.txt")

    // Trains a LDA model.
    /**
      * k: 主题数，或者聚类中心数
      * DocConcentration：文章分布的超参数(Dirichlet分布的参数)，必需>1.0，值越大，推断出的分布越平滑
      * TopicConcentration：主题分布的超参数(Dirichlet分布的参数)，必需>1.0，值越大，推断出的分布越平滑
      * MaxIterations：迭代次数，需充分迭代，至少20次以上
      * setSeed：随机种子
      * CheckpointInterval：迭代计算时检查点的间隔
      * Optimizer：优化计算方法，目前支持"em", "online" ，em方法更占内存，迭代次数多内存可能不够会抛出stack异常
      */
    val lda = new LDA().setK(8)
      .setTopicConcentration(10)
      .setDocConcentration(10)
//      .setOptimizer("online")
//      .setOptimizer("em")
      .setCheckpointInterval(10)
      .setMaxIter(50)
    val model = lda.fit(dataset)

    val ll = model.logLikelihood(dataset)
    val lp = model.logPerplexity(dataset)
    println(s"The lower bound on the log likelihood of the entire corpus: $ll")
    println(s"The upper bound on perplexity: $lp")

    // Describe topics.
    //主体分布排序，每一个主题的词典权重排序。
    val topics = model.describeTopics(3)
    println("The topics described by their top-weighted terms:")
    topics.show(false)

    //（2） topicsMatrix: 主题-词分布，相当于phi。
    println("Learned topics (as distributions over vocab of " + model.vocabSize + " words):")
    val topicsMat = model.topicsMatrix
//    topicsMat.rowIter.foreach(x => {
//      x.toArray.foreach(s => print(s"$s \t"))
//      println()
//    })
    for (topic <- Range(0, 8)) {
      print("Topic " + topic + ":")
      for (word <- Range(0, model.vocabSize)) { print(" " + topicsMat(word, topic)); }
      println()
    }
    println("topicsMatrix")
    println(topicsMat.toString())

    // Shows the result.
    val transformed = model.transform(dataset)
    transformed.show(false)
    // $example off$

    spark.stop()
  }
}
