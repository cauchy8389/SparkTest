package RDDScala

import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * RDD数据集中算子
  * 所谓算子 其实就是RDD接口提供的方法
  * Created by Administrator on 2018/8/25.
  */
object MyRDD {
  def main(args: Array[String]): Unit = {
    //System.setProperty("hadoop.home.dir", "E:\\hadoop-2.6.0-cdh5.8.5")

    val conf  = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("wordcount")
    val sc = new SparkContext(conf)
    //算子
    //转化算子
    //行动算子
    //根据返回值可以判断  如果返回值类型是RDD类型，那么就是转化算子
    //如果返回值是其他类型，那么就是行动算子
    //val sourceRDD  = sc.parallelize(List(5, 1, 7, 8, 3,4))
    //val sourceRDDA = sc.parallelize(List(1, 2, 3))
    //val sourceRDDB = sc.parallelize(List(3, 4, 5))

    //一 普通RDD
    /////////////////////转化操作/////////////////////////////////
    //map 操作
    //def map[U: ClassTag](f: T => U): RDD[U]
    //T 表示的是RDD每个元素类型
    //U 表示经过提供的高阶函数 返回的类型
     // sourceRDD.map(ele => "candle" + ele).foreach(println(_))

    //def flatMap[U: ClassTag](f: T => TraversableOnce[U]): RDD[U]
    //T 元素的类型
    //TraversableOnce[U] 生成一个集合
    //通过集合中的元素 组成一个新的RDD。通常用于扩展
    //sourceRDD.flatMap(x => 1 to x).foreach(println(_))

    //def filter(f: T => Boolean): RDD[T]
    //针对每一个元素进行高阶函数处理，如果返回为true则这个元素留下
    //如果为false 那么就删除
    //sourceRDD.filter(_ % 2 == 1).foreach(println)
    //sourceRDD.distinct().foreach(println)

    /*def sample(
    withReplacement: Boolean,
    fraction: Double,
    seed: Long = Utils.random.nextLong): RDD[T]
    *
    */
    //sourceRDD.sample(false, 0.25).foreach(println)



    //union
    //def union(other: RDD[T]): RDD[T]
    //每一个rdd作为一个分区
    //sourceRDDA.union(sourceRDDB).saveAsTextFile("E:\\union")

    //def intersection(other: RDD[T]): RDD[T]
    //求出两个RDD中共同的元素
    //sourceRDDA.intersection(sourceRDDB).foreach(println)

    //移除一个RDD 中的内容（例如移
    //除训练数据）
    //sourceRDDA.subtract(sourceRDDB).foreach(println)

    //笛卡尔积
    //两个RDD中 每个元素 两两组合生成 新的pairRDD
    //sourceRDDA.cartesian(sourceRDDB).foreach(println)

    //////////////////////行动操作/////////////
    //def foreach(f: T => Unit): Unit
    //T 表示的是RDD每个元素类型
    //无返回值
    //针对每个元素进行高阶函数操作，无返回值，一般用作输出
    //sourceRDD.foreach(println)

    //返回RDD 中的所有元素  存放在数组中
    //def collect(): Array[T]
    //企业环境 不要使用
    //把所有的节点 中数据 传递到一个节点 极其容易造成内存溢出
    //val arr = sourceRDDA.collect()


    //count
    //返回元素的个数
    //println(sourceRDD.count())

    //def countByValue()(implicit ord: Ordering[T] = null): Map[T, Long]
    //统计元素个数
    //val map = sourceRDD.countByValue()
    //println(map)

    //def take(num: Int): Array[T]
    //从左往右 取前三个元素
    //val arr =  sourceRDD.take(3)
    //for(tmp <- arr) println(tmp)

    //取前三个元素，默认升序，取最小的三个值
    //sourceRDD.takeOrdered(3).foreach(println)

    //sourceRDD.top(3).foreach(println)

    //和sample一样 ，只不过takeSample 是一个转化操作
   // sourceRDD.takeSample()

    ///////////////////////////////////////////////////////////
    //二.pairRDD   专门针对  二元组
    //tuple(xxx, xx)
    ////////////////转化操作/////////////////////
    //val pairRDD = sc.parallelize(List((1, 2),(3, 4),(4, 3),(3, 6)))
    val p1 = sc.parallelize(List((1, 2), (3, 4), (3, 6)))
    //p2 不是二元组  有两个int的元素
   // val p2 = sc.parallelize(List(3, 9))
    val p2 = sc.parallelize(List((3, 9), (3, 10)))
    //注意  pairRDD只能用作于 二元组
    //也可以把pairRDD当做一个普通RDD来处理，即上面普通RDD算子都是可以使用的

    //转化操作
    //def reduceByKey(func: (V, V) => V): RDD[(K, V)]
    //以二元组中key  （key, value）
    //输入相同的key的value值，返回值是RDD
    //pairRDD.reduceByKey(_+_).foreach(println)

    //操作是行动操作
    //println(pairRDD.reduce((v1,v2) =>(v1._1+v2._1, v1._2+v2._2)))

    //对具有相同键的值进行分组
    //groupbyKey

    //def groupByKey(): RDD[(K, Iterable[V])]
    //把相同key的value放到一个迭代器中
    //(3,[4, 6])
    //pairRDD.groupByKey().foreach(println)


    //def mapValues[U](f: V => U): RDD[(K, U)]
    //只操作value key不做任何变化
    //pairRDD.mapValues(_*2).foreach(println)

    //1 2 3 4  3 6
    //1 [1, 2, 3]  3 [1, 2, 3, 4]
    //(1, 1) (1, 2) (1,3) .....
    //pairRDD.flatMapValues(1 to _).foreach(println)

    //pairRDD.keys.foreach(println)
    //pairRDD.values.foreach(println)

    //根据Key进行排序
    //true 升序  false 降序
    //pairRDD.sortByKey(false).foreach(println)

    //sortBy 针对是普通的RDD
    //以元组中 第二个元素排序，true升序  false降序
    //pairRDD.sortBy(x=>x._2, true).foreach(println)
    /////////////////行为操作/////////////////
    //p1.subtractByKey(p2).foreach(println)

    //p1.join(p2).foreach(println)

    //p1.leftOuterJoin(p2).foreach(println)

    //按照key进行聚合
    //key ,([4, 6], [9, 10])
    //p1.cogroup(p2).foreach(println)

    //行动操作
    // def countByKey(): Map[K, Long]
    //统计 key的个数  针对pairRDD 二元组
    p1.countByKey().foreach(println)
    //针对普通RDD
    p1.countByValue().foreach(println)

    //collectAsMap() 将结果以映射表的形式返回，以便查寻
    //会发生把所有节点中的数据 聚集到一个节点。

    p1.lookup(3).foreach(println)

  }
}
