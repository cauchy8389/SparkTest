package SparkStreamingScala

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Administrator on 2018/9/16.
  */
object WindowsWordCountScala {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "E:\\hadoop-2.6.0-cdh5.8.5")
    val conf  = new SparkConf()
    conf.setMaster("local[2]") //默认只开一个线程
    conf.setAppName("spark streaming socket")

    val ssc =   new StreamingContext(conf, Seconds(5))

    ssc.checkpoint("hdfs://candle.hwua.com:9000/user/candle/spark/wd_checkpoint")

    val source = ssc.socketTextStream("candle.hwua.com", 9999)

    source
      .flatMap(line =>line.split(" "))
      .map(word => (word, 1))
      .reduceByKeyAndWindow((v1:Int, v2:Int)=> v1+v2, Seconds(60), Seconds(10))
      .print()

    ssc.start()
    ssc.awaitTermination()

  }
}
