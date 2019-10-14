package RDDScala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/9/2.
  */
object Persist {
  def main(args: Array[String]): Unit = {
    //持久化
    //RDD 延迟计算：只有在行动操作，才会触发计算。转化操作不会触发计算
    //     重新计算：一旦转化操作执行完毕，那么这个RDD就不存在

    System.setProperty("hadoop.home.dir", "E:\\hadoop-2.6.0-cdh5.8.5")
    val conf  = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("persist")
    val sc = new SparkContext(conf)

    val sourceRDD = sc.parallelize(List(1, 2 , 3, 4, 5))

    //转化操作 并不会触发计算
    val mapRDD =  sourceRDD.map(x => println("hello " + x))

    //sourceRDD -> mapRDD ->集合
    //重新计算  内存资源和cpu资源权衡
    //持久化
    //内存资源和CPU资源发生冲突，优先是要满足内存。
    //把这个RDD持久化，就是说把这个RDD存放在内存中或者本地磁盘
    //并不会释放掉
    //mapRDD.cache()

    //注意：1.持久化的操作不会触发转化操作  cache 和persist不是算子
    // 2.持久化 只发生在  cache被调用的后的第一次计算
    mapRDD.collect()
    mapRDD.cache()
    mapRDD.collect()

    //清除缓存的操作
    mapRDD.unpersist()

    /*
     使用persist 可以传入如下参数
     清除缓存的操作  unpersist中调用
  val NONE = new StorageLevel(false, false, false, false)
     _2 表示缓存的结果 备份两份
     本地磁盘 持久化
  val DISK_ONLY = new StorageLevel(true, false, false, false)
  val DISK_ONLY_2 = new StorageLevel(true, false, false, false, 2)
     内存中 持久化
  val MEMORY_ONLY = new StorageLevel(false, true, false, true)
  val MEMORY_ONLY_2 = new StorageLevel(false, true, false, true, 2)
     内存中持久化  并且序列化
  val MEMORY_ONLY_SER = new StorageLevel(false, true, false, false)
  val MEMORY_ONLY_SER_2 = new StorageLevel(false, true, false, false, 2)
     优先内存 内存不够 使用本地磁盘
  val MEMORY_AND_DISK = new StorageLevel(true, true, false, true)
  val MEMORY_AND_DISK_2 = new StorageLevel(true, true, false, true, 2)
     优先内存 内存不够 使用本地磁盘  并且序列化
  val MEMORY_AND_DISK_SER = new StorageLevel(true, true, false, false)
  val MEMORY_AND_DISK_SER_2 = new StorageLevel(true, true, false, false, 2)
  少用   堆外空间  除非非常了解 jvm
  val OFF_HEAP = new StorageLevel(false, false, true, false)

  策略：
  1.CPU资源和内存资源，两者都紧张情况下，优先考虑节省内存
  2.优先使用MEMORY_ONLY，如果可以缓存所有数据的话，那么就优先使用这种策略。
    因为纯内存操作速度最快。
  3.其次考虑 MEMORY_ONLY_SER，可以对数据进行压缩
  4.最后，能不使用DISK相关的策略，就不去使用。大多时候，从磁盘读取数据，
    所花费的时间比重新计算花费的时间还要多。
     */
    mapRDD.persist()



  }
}
