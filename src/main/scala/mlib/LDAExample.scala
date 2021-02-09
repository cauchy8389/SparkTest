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

    //模型的评价指标：ogLikelihood，logPerplexity
    //（1）根据训练集的模型分布计算的log likelihood，越大越好。
    val ll = model.logLikelihood(dataset)
    //（2）Perplexity评估，越小越好
    val lp = model.logPerplexity(dataset)
    println(s"The lower bound on the log likelihood of the entire corpus: $ll")
    println(s"The upper bound on perplexity: $lp")

    // Describe topics.
    //主体分布排序，每一个主题的词典权重排序。
    val topics = model.describeTopics(3)
    println("The topics described by their top-weighted terms:")
    topics.show(false)

    /**主题    主题包含最重要的词语序号                     各词语的权重
        +-----+-------------+------------------------------------------+
        |topic|termIndices  |termWeights                               |
        +-----+-------------+------------------------------------------+
        |0    |[5, 4, 0, 1] |[0.21169509638828377, 0.19142090510443274]|
        |1    |[5, 6, 1, 2] |[0.12521929515791688, 0.10175547561034966]|
        |2    |[3, 10, 6, 9]|[0.19885345685860667, 0.18794498802657686]|
        +-----+-------------+------------------------------------------+
      */

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
    /** label是文档序号 文档中各主题的权重
        +-----+--------------------------------------------------------------+
        |label|topicDistribution                                             |
        +-----+--------------------------------------------------------------+
        |0.0  |[0.523730754859981,0.006564444943344147,0.46970480019667477]  |
        |1.0  |[0.7825074858166653,0.011001204994496623,0.206491309188838]   |
        |2.0  |[0.2085069748527087,0.005698459472719417,0.785794565674572]   |
        ...

      */
    // $example off$

    spark.stop()
  }
}
