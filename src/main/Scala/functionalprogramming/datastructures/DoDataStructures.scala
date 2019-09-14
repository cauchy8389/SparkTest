package functionalprogramming.datastructures

import functionalprogramming.datastructures.List._

object DoDataStructures {
  def main(args: Array[String]): Unit = {
    val ls = List(1,2,3,4,5,6)
    println(sum(ls))

    val lsd = List(1.0,2.0,3.0,4.0)
    println(product(lsd))

    val lsd0 = List(0.0,2.0,3.0,4.0)
    println(product(lsd0))

    println(apply(1,2,3))

    println(x)

    println("append:" + append(lsd,lsd0))

    val lsSum = List(1,2,3,4)
    println(sum2(lsSum))

    println(product2(lsd))

    println("tail:" + tail(lsd))

    println("setHead:" + setHead(lsSum, 8))

    println("drop:" + drop(lsSum, 2))

    //val func = (i: Int) => i <= 3
    println("dropWhile:" + dropWhile(lsSum, (i: Int) => i <= 3))

    println("init:" + init(lsSum))
    println("init2:" + init2(ls))

    println("length:" + length(ls))

    println("sum3:" + sum3(ls))
    println("product3:" + product3(lsd))
    println("length2:" + length2(lsd))

    println("reverse:" + reverse(lsd))

    println("foldRightViaFoldLeft:" + foldRightViaFoldLeft(lsd))
  }
}
