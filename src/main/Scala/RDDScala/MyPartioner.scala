package RDDScala

import org.apache.spark.Partitioner

/**
  * Created by Administrator on 2018/9/2.
  */
//自定义分区
class MyPartioner(val numPartition:Int) extends  Partitioner{
  override def numPartitions: Int = numPartition
  //分区的个数

  //需要实现的算法。返回值int，根据返回值来确定最终分区
  override def getPartition(key: Any): Int = {
    if(key.toString.toInt >=4) {
      0
    }else if(key.toString.toInt >=2 && key.toString.toInt < 4) {
      1
    }else {
      2
    }
  }
}
