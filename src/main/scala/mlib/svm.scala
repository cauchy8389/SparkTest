package mlib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{MinMaxScaler, VectorIndexer}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

import scala.collection.immutable.SortedSet

object svm {

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "E:\\hadoop-2.6.0-cdh5.8.5")

    //1 构建Spark对象
    val conf = new SparkConf().setMaster("local").setAppName("SVMExample")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.setCheckpointDir("hdfs://192.168.1.51:9000/user/zhy/spark/wd_checkpoint")
    Logger.getRootLogger.setLevel(Level.WARN)

//    // 读取样本数据1，格式为LIBSVM format
//    val data = MLUtils.loadLibSVMFile(sc, "hdfs://zhy.cauchy8389.com:9000/user/zhy/mlib/sample_libsvm_data.txt")
//
//    //样本数据划分训练样本与测试样本
//    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
//    val training = splits(0).cache()
//    val test = splits(1)
//
//    //新建逻辑回归模型，并训练
//    val numIterations = 100
//    val model = SVMWithSGD.train(training, numIterations)
//
//    //对测试样本进行测试
//    val predictionAndLabel = test.map { point =>
//      val score = model.predict(point.features)
//      (score, point.label)
//    }
//    val print_predict = predictionAndLabel.take(20)
//    println("prediction" + "\t" + "label")
//    for (i <- 0 to print_predict.length - 1) {
//      println(print_predict(i)._1 + "\t" + print_predict(i)._2)
//    }
//
//    // 误差计算
//    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
//    println("Area under ROC = " + accuracy)
//
//    //保存模型
//    val ModelPath = "/user/zhy/svm_model"
//    model.save(sc, ModelPath)
//    val sameModel = SVMModel.load(sc, ModelPath)


    // $example on$
    val data = spark.read.format("libsvm").load("file:///F:/download/MLlib机器学习/数据/sample_libsvm_data.txt")

    val indexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexed")
      .setMaxCategories(6)

    val indexerModel = indexer.fit(data)

    val categoricalFeatures: Set[Int] = indexerModel.categoryMaps.keys.toSet
    println(s"Chose ${categoricalFeatures.size} " +
      s"categorical features: ${categoricalFeatures.mkString(", ")}")

    println(s"sorted categorical features:${categoricalFeatures.toList.sorted.mkString(", ")}")

    // Create new column "indexed" with categorical values transformed to indices
    val indexedData = indexerModel.transform(data)
    indexedData.show()
    // $example off$
    println("indexed:")
    indexedData.select("indexed").collect().foreach(x=>println(x))
    println("features:")
    indexedData.select("features").collect().foreach(x=>println(x))

    //-----------start MinMaxScaler---------------------------------------------------------------------

    val dataFrame = spark.createDataFrame(Seq(
      (0, Vectors.dense(1.0)),
      (1, Vectors.dense(2.0)),
      (2, Vectors.dense(99.0))
    )).toDF("id", "features")

    val scaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setMin(0)
      .setMax(1)

    // Compute summary statistics and generate MinMaxScalerModel
    val scalerModel = scaler.fit(dataFrame)

    // rescale each feature to range [min, max].
    val scaledData = scalerModel.transform(dataFrame)
    println(s"Features scaled to range: [${scaler.getMin}, ${scaler.getMax}]")
    scaledData.select("features", "scaledFeatures").show()

    //-----------end MinMaxScaler----------------------------

    spark.stop()
  }

}
