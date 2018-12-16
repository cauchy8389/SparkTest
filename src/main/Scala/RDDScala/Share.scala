package RDDScala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/9/2.
  */
object Share {
  def main(args: Array[String]): Unit = {
    //共享变量
    //默认情况下，如果一个算子使用某个外部变量，由于算子中是采用值传递，
    //那么 这个变量值拷贝到算子所在的task中。每个task只能操作自己的那份变量
    //副本。

    System.setProperty("hadoop.home.dir", "E:\\hadoop-2.6.0-cdh5.8.5")
    val conf  = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("share")
    val sc = new SparkContext(conf)

    val sourceRDD = sc.parallelize(List(1, 2, 3, 4, 5))

    //var sum = 0
    var sum = sc.accumulator(0)

    //需要用到外部变量
    //累加器accumulator。主要用于多个节点对一个变量进行共享性的操作
    sourceRDD.map(x => {println("sum:"+sum);  sum.add(x)}).collect()


    println(sum)

    //广播变量
    //默认的情况下，如果传递给RDD的算子的变量。默认的情况下，每个分区都会有一个
    //备份。

    //广播变量
    var factor = sc.broadcast(1)

    //广播变量只读的
    sourceRDD.map(num => {  num + factor.value}).foreach(println)


    //数值RDD的操作
    println(sourceRDD.count())

    println(sourceRDD.mean())

    //自定义的累加器 AccumulatorParam
    //统计 用户访问session 时长
    //1-3s   3-5s  5....    100个时间段。
    //系统需要定义100个累加器？？
    //可以自定义累加器


  }
}
