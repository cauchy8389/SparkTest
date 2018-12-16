package SparkStreamingScala

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Administrator on 2018/9/16.
  */

/**
  * 通过 网络
  * 监控一个IP端口
  * 只要往该端口  写入数据   然后就做一个单词统计的操作
  */
object WordCountLocal {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\hadoop-2.6.0-cdh5.8.5")
    val conf  = new SparkConf()
    //本地测试环节  必须开启两个线程
    //一个线程用于读取数据  一个线程用于处理数据
    conf.setMaster("local[2]") //默认只开一个线程
    conf.setAppName("spark streaming socket")

    //(conf : org.apache.spark.SparkConf, batchDuration
    //batchDuration 时间间隔   就是每隔五秒 生成一个RDD  batch
    val ssc =   new StreamingContext(conf, Seconds(5))

    //输入源   监控网络端口  以文本形式 接受数据
    val source = ssc.socketTextStream("candle.hwua.com", 9999)
    //处理过程  就是RDD处理
    //每隔5s 会对rdd进行一次处理
    source
      .flatMap(line =>line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_+_)
      .print()

    //开启任务
    ssc.start()

    //等待任务执行，这个必须调用，不然主进程就会结束
    ssc.awaitTermination()

  }
}
