package RDDScala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 实现一个单词统计
  * RDD集合又分为两种
  * 一种是针对于普通的RDD 即RDD中的元素是一个
  * 另外一种 针对于 pairRDD RDD中的元素是一个键值对
  *
  * scala支持隐式转化，不需要特定转化。java中需要调用对应的方法
  * 进行转换
  * Created by Administrator on 2018/8/25.
  */

object WordCount {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\hadoop-2.6.0-cdh5.8.5")
    //配置
    val conf  = new SparkConf()
    //需要指明  运行的资源管理器
    //如果是在本地
    conf.setMaster("local")
    //任务的名称
    conf.setAppName("wordcount")
    //new一个驱动  入口
    val sc = new SparkContext(conf)

    //有了这个驱动  就可以获取数据
    //RDD获取的数据源 有两种形式 一种是内部 另外一种是外部
    //sc.textFile()  从外部输入源获取数据
    //sc.parallelize()   从内部输入源获取数据 指的就是内存中数据
    //一般这种形式 都是测试时候 启用。因为从内部获取数据，所有的数据
    //都是在本地。
    //转化操作
    //val sourceRDD  =
    //    sc.parallelize(List("aa bb cc dd", "aa aa cc cc", "dd dd cc cc"))
    val sourceRDD =
      sc.textFile("E:\\text.txt") //从外部获取数据

    //sourceRDD就是一个数据集 RDD中提供非常丰富算子操作，
    // 算子操作其实就是RDD的方法

    //把元素 进行扩展
    //"aa bb cc dd" -> "aa"  "bb"  "cc"  "dd"
    val flatMapRDD = sourceRDD.flatMap(line => line.split(" "))

    //aa ->(aa, 1)
    val mapRDD = flatMapRDD.map(word=>(word, 1))

    //针对pairRDD  按照key值 进行聚合
    //相同的key的value进行聚合
    val reduceByKeyRDD =  mapRDD.reduceByKey((v1, v2) =>v1+v2)

    //会对RDD中每一个元素进行操作
    //没有返回值
    reduceByKeyRDD.foreach(word => println(word._1 + ":" + word._2))
   //输出的结果  和mr进行比较
    //mr  input ->map ->shuffle ->reduce ->output  shuffle时候对键值对排序
    //spark input ->转化操作.....->行为操作  计算颗粒度比较小
    //编程更加方便，另外spark shuffle阶段可以选择排序，也可以不用排序



    /*
    总的来说，每个Spark 程序或shell 会话都按如下方式工作。
(1) 从外部数据创建出输入RDD。
(2) 使用诸如filter() 这样的转化操作对RDD 进行转化，以定义新的RDD。
(3) 告诉Spark 对需要被重用的中间结果RDD 执行persist() 操作。
(4) 使用行动操作（例如count() 和first() 等）来触发一次并行计算，Spark 会对计算进行
优化后再执行。
     */
  }
}
