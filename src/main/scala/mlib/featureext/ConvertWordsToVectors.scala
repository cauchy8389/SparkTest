package mlib.featureext

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{Row, SparkSession}

object ConvertWordsToVectors{
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "E:\\hadoop-2.6.0-cdh5.8.5")
    val file = "file:///F:/CodingResource/BigData/spark-ml-branch-ed2/Chapter_04/data/text8_10000"
    val conf = new SparkConf().setMaster("local").setAppName("Word2Vec example")
    val spark = SparkSession.builder.config(conf).getOrCreate()
    val input = spark.sparkContext.textFile(file).flatMap(line => line.split(" ") )

    val documentDF = spark.createDataFrame(input.map(x => List(x))).toDF("text")

//    val word2vec = new Word2Vec()
//    val model = word2vec.fit(input)
//    val vectors = model.getVectors
//    vectors foreach ( (t2) => println (t2._1 + "-->" + t2._2.mkString(" ")))


    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)
    val model = word2Vec.fit(documentDF)

    val result = model.transform(documentDF)
    result.collect().foreach { case Row(text: Seq[_], features: Vector) =>
      println(s"Text: [${text.mkString(", ")}] => \nVector: $features\n") }


    val documentDF2 = spark.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")

    // Learn a mapping from words to Vectors.
    val word2Vec2 = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)
    val model2 = word2Vec2.fit(documentDF2)

    val result2 = model2.transform(documentDF2)
    result2.collect().foreach { case Row(text: Seq[_], features: Vector) =>
      println(s"Text: [${text.mkString(", ")}] => \nVector: $features\n") }
    // $example off$

    spark.stop()

  }
}