package mlib.featureext

// $example on$
import org.apache.spark.ml.feature.{HashingTF, IDF, OneHotEncoderEstimator, Tokenizer, VectorAssembler}
// $example off$
import org.apache.spark.sql.SparkSession
/**
  * @author Rajdeep Dua
  *         March 4 2016
  */
object TfIdfSample{
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\hadoop-2.6.0-cdh5.8.5")
    val spark = SparkSession
      .builder.master("local")
      .appName("TfIdfExample")
      .getOrCreate()

    // $example on$
    val sentenceData = spark.createDataFrame(Seq(
      (0.0, "Hi I heard about Spark"),
      (0.0, "I wish Java could use case classes"),
      (1.0, "Logistic regression models are neat")
    )).toDF("label", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)

    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(16)

    val featurizedData = hashingTF.transform(wordsData)
    // alternatively, CountVectorizer can also be used to get term frequency vectors
    featurizedData.select("label", "rawFeatures").show(false)
    featurizedData.show(false)


    val doc = spark.createDataFrame(Seq(
      (0.0, "a a b b c d")
    )).toDF("label", "sentence")
    val wordsData_2 = tokenizer.transform(doc)
    val doc_1=hashingTF.transform(wordsData_2)
    doc_1.show(false)
    print(doc_1.collect())

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.select("label", "features").show(false)

//    var wordMap = wordsData.select("words").rdd.flatMap { row => //保存所有单词以及经过hasingTf后的索引值：{单词1:索引,单词2:索引...}
//    {
//      row.getAs[Seq[String]](0).map { w => (hashingTF.indexOf(w), w) }
//    }
//    }.collect().toMap
//
//    val keyWords = rescaledData.select("features").rdd.map { x =>
//    {
//      val v = x.getAs[org.apache.spark.ml.linalg.SparseVector](0)//idf结果以稀疏矩阵保存
//      v.indices.zip(v.values).sortWith((a, b) => { a._2 > b._2 }).take(10).map(x => (wordMap.get(x._1).get, x._2))//根据idf值从大到小排序，取前10个，并通过索引反查到词
//    }
//    } //[(文章1的关键词索引1:tf-idf值,文章1的关键词索引2:tf-idf值),(文章n的关键词索引1:tf-idf值,文章n的关键词索引2:tf-idf值)...],每组()表示一个新闻的关键词
//
//    keyWords.collect().foreach(x => {
//      println(x._1 + ":" + x._2 + " ")
//    })

//    val df = spark.createDataFrame(Seq(
//      (Seq(0.0, 1), 1.0),
//      (Seq(1.0, 2), 0.0),
//      (Seq(2.0, 2), 1.0),
//      (Seq(0.0, 2), 2.0),
//      (Seq(0.0, 2), 1.0),
//      (Seq(2.0, 1), 0.0)
//    )).toDF("categoryIndex1", "categoryIndex2")

    val df = spark.createDataFrame(Seq(
      (0.0, 1.0),
      (1.0, 0.0),
      (2.0, 1.0),
      (0.0, 2.0),
      (0.0, 1.0),
      (2.0, 0.0)
    )).toDF("categoryIndex1", "categoryIndex2")

    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array("categoryIndex1", "categoryIndex2"))
      .setOutputCols(Array("categoryVec1", "categoryVec2"))
    val model = encoder.fit(df)

    val encoded = model.transform(df)
    encoded.show()

    val va = new VectorAssembler().setInputCols(Array("categoryVec1", "categoryVec2")).setOutputCol("features")
    val df1 = va.transform(encoded)
    df1.show(false)

    encoded.foreach(x=>{
      val ary = x.get(3)
      println(ary)
    })

    // $example off$

    spark.stop()
  }
}