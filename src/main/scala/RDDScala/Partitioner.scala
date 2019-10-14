package RDDScala


import org.apache.spark.{HashPartitioner, RangePartitioner, SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/9/2.
  */
object Partitioner {
  def main(args: Array[String]): Unit = {
    //分区
    //分区的个数 将决定  有多少个线程并发。同时也将觉得有多少reduce，那么就有多少个输出
    System.setProperty("hadoop.home.dir", "E:\\hadoop-2.6.0-cdh5.8.5")
    val conf  = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("partitioner")
    val sc = new SparkContext(conf)
    //第二个参数就是分区数量，默认是1个分区
    //第一种分区方式，就是按照RDD中的值的顺序取值
    //对于分区数该如何确定？？？
    //建议分数总数是cpu总数2~3倍
    //val sourceRDD = sc.parallelize(List(1, 6, 7, 8, 9, 10, 2, 3, 4, 5),3)


    //sourceRDD.saveAsTextFile("E://mytest")

    val sourceRDD = sc.parallelize(List((9, 1), (3, 2), (2,3),
      (6, 1), (4, 2), (7,3),
      (1, 1), (2, 2), (3,3),
      (8, 1), (4, 2), (7,3),
      (7, 1), (3, 2), (6,3),
      (2, 1), (9, 2), (1,3),
      (8, 1), (3, 2), (4,3)))

    //hashPartitioner分区
    //专门针对 pairRDD
    //根据RDD中key值的hashCode的值将数据取模后得到该key值对应的RDD分区id
    //  可以让相同的KEY存放到一个分区中

    //HashPartitioner
    //数据倾斜，有些情况下  一个分区中  节点过多。
    //sourceRDD.partitionBy(new HashPartitioner(3)).saveAsTextFile("E://mytest2")

    //针对key   进行range操作   所谓的range就是 统计出key个数 然后是排序
    //尽量保证负载均衡
    //1 3   2  6  3 1  4 2  5 3   2个分区
    //优先形同的key存放到同一个分区中，然后尽量 数据平均
    //sourceRDD.partitionBy(new RangePartitioner(3,sourceRDD)).saveAsTextFile("E://mytest3")

    //实现一个自定义分区
    sourceRDD.partitionBy(new MyPartioner(3)).saveAsTextFile("E://mytest4")
  }
}
