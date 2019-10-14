package functionalprogramming.datastructures

object DoDataStructures {

  def main(args: Array[String]): Unit = {
    import functionalprogramming.datastructures.List._

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

    println("foldRightViaFoldLeft:" + foldRightViaFoldLeft(lsd, 0.0)((i, j) => (i + j)))
    println("foldRightViaFoldLeft_1:" + foldRightViaFoldLeft_1(lsd, 0.0)((i, j) => (i + j)))
    println("foldLeftViaFoldRight:" + foldLeftViaFoldRight(lsd, 0.0)((i, j) => (i + j)))
    println("appendViaFoldRight:" + appendViaFoldRight(List(1,2,3),List(4,5,6)))

    println("concat:" + concat(List(List(1,2,3),List(4,5,6))))
    println("add1:" + add1(List(1,2,3,4)))
    println("doubleToString:" + doubleToString(List(1,2,3,4)))

    println("map:" + map(List(1,2,3,4))(i => i + 1.0))
    println("filter:" + filter(List(1,2,3,4))(i => i % 2 == 0))
    println("flatMap:" + flatMap(List(1,2,3,4))(i => {
        val buf = new collection.mutable.ListBuffer[Double]
        for (j <- 0 to i) {
          buf += j
        }
        List(buf.toList: _*)
      }
    ))
    println("addPairwise: " + addPairwise(List(1,2,3),List(4,5,6)))
    println("zipWith: " + zipWith(List(1,2,3),List(4,5,6))((a,b) => {
        a.toString + ":" + b.toString
      }
    ))
    println("startsWith: " + startsWith(List(1,2,3),List(1,2)))
    println("hasSubsequence: " + hasSubsequence(List(1,2,3),List(2,3)))

    println("--Tree----Tree----Tree----Tree----Tree----Tree-----------")

    import functionalprogramming.datastructures.Tree._

    val tree1 = Branch(Branch(Leaf(1),Branch(Leaf(2),Leaf(5))), Leaf(6))
    println("size:" +  Tree.size(tree1))
    println("\"A\"+:\"B\": " + ("A" +: "B"))
    println("maximum:" +  maximum(tree1))
    println("depth:" +  Tree.depth(tree1))
    println("map:" +  Tree.map(tree1)(i => i+1.0))
    println("fold:" +  Tree.fold(tree1)(i => i+1.0)((a,b) => a+b))
    println("sizeViaFold:" +  sizeViaFold(tree1))
    println("maximumViaFold:" +  maximumViaFold(tree1))
    println("depthViaFold:" +  depthViaFold(tree1))
    println("mapViaFold:" +  mapViaFold(tree1)(i => i+1.0))
  }
}
