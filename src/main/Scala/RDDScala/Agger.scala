package RDDScala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 聚合操作
  *
  * Created by Administrator on 2018/8/25.
  */
object Agger {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\hadoop-2.6.0-cdh5.8.5")
    val conf  = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("wordcount")
    val sc = new SparkContext(conf)

    val sourceRDD  = sc.parallelize(List(5, 1, 7, 8, 3,4),2)
    sourceRDD.foreach(println)

    //聚合操作
    // def reduce(f: (T, T) => T): T
    //转化操作
    //相加的操作
    //reduce 和fold聚集   元素的类型不变
      //5 + 1
    //println(sourceRDD.reduce((v1, v2) => v1 + v2))
    //println(sourceRDD.reduce(_+_))

    //针对每个分区 会有一个初始值
    //针对整个RDD 也会有一个初始值

    // 1+( (1 + 5 + 1 + 7)      +    (1 + 8 +3 + 4))

    println(sourceRDD.fold(1)(_+_))



    //这种聚合，最终聚合的结果类型和原来是不一样的。
    //考虑 初始值为何？
    //分区内的操作
    //分区间的操作
    //求平均值
    //integer类型  ==》  (总和，元素个数)
    //def aggregate[U: ClassTag](zeroValue: U)
    //  (seqOp: (U, T) => U, combOp: (U, U) => U): U
    //T 类型 就是元素类型
    //U 类型 就是经过聚合之后类型。
    //                  (5 + 1 + 7)      +    (  8 +3 + 4)
    // 分区内的操作      13 , 3                 15 ，3
    // 分区间的操作      13+15  3+3
    //seqOp  分区内的操作
    //combOp  分区间的操作
    //转化操作
    val  aggregateRDD = sourceRDD.aggregate(0,0)(
      (acc, value) =>(acc._1 +value, acc._2 + 1 ),
      (acc1, acc2) =>(acc1._1 + acc2._1, acc1._2 + acc2._2)
    )

    //28 /6
    val avg = aggregateRDD._1/(aggregateRDD._2*1.0)
    println(avg)
    //combineByKey
    //reduceByKey


    val sourceRDD1 = sc.parallelize(List(("class1", 89),("class2", 76), ("class1", 69),
      ("class2", 90)))

    //针对 键值对 根据key 进行聚合 第一种聚合，聚合的结果类型保持不变
    sourceRDD1.reduceByKey((v1,v2)=> v1+v2 ).foreach(println)

    //如果聚合时候，结合的结果和value类型是不一致
    // "class1", (总和,个数)
    /*
      V 是value的类型   C就是聚合之后的类型
      def combineByKey[C](
      createCombiner: V => C,   这个高阶函数 提供了 由value是如何转化成C类型
      mergeValue: (C, V) => C,  C和Value操作 生成类型 用于分区内操作
      mergeCombiners: (C, C) => C)  分区间的操作
      : RDD[(K, C)]

      针对普通的RDD  reduce 和aggregate 返回值不是RDD类型
      所以是转化操作

      针对pairRDD  reduceByKey 和combineByKey 返回值仍然是RDD类型
      所以是行动操作
     */
    val  combineByKeyRDD = sourceRDD1.combineByKey(
      v => (v,1),
      (c:(Int, Int), v) =>(c._1 + v, c._2 + 1),
      (c1:(Int, Int), c2:(Int, Int)) => (c1._1+c2._1, c1._2 + c2._2)
    )

  combineByKeyRDD.map(x =>  (x._1, x._2._1/x._2._2)).foreach(println)
  //combineByKeyRDD.map((key,value) =>(key, value._1/value._2) )
    //这种写法错误，因为解析器会把（key，value）解析成 map中的两个参数

    //偏函数  scala语法
    combineByKeyRDD.map{case (key, value)=> (key, value._1/value._2)}.foreach(println)


  }





}
