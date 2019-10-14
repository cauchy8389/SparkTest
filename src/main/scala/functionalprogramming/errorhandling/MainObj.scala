package functionalprogramming.errorhandling

object MainObj {
  def main(args: Array[String]): Unit = {
    //Option.failingFn(5)
    println(Option.failingFn2(5));
    println(Option.mean(List(1,2,3,4)))
    println(Option.mean(Nil))

    println("variance: " + Option.variance(List(1,2,3,4)))
    println("map2: " + Option.map2(Some(1),Some(2))((i,j) => i + j))

    println("sequence: " + Option.sequence(List(Some(1),Some(2),Some(3),Some(4))))
    println("traverse: " + Option.traverse(List(1,2,3,4))(i => Some(i + 1.0)))

    println("sequenceViaTraverse: " + Option.sequenceViaTraverse(List(Some(1),Some(2),Some(3),Some(4))))


    println("Either: ----------------------------------")

    println("mean: " + Either.mean(IndexedSeq(1.0,2.0,3.0,4.0)))
    println("safeDiv: " + Either.safeDiv(1,0))
    println("Try: " + Either.Try(f(6)))

    def doubles(x: => Int) = {
      println("haven't calculate !")
      println("Now doubling " + x)
      x*2
      x + 2
    }

    def f(x: Int): Int = {
      println(s"Calling f($x)")
      x + 1
    }

    println("doubles:" + doubles(f(2)))

    println("traverse:" + Either.traverse(List(1,2,3,4))(
      i => if(i % 2 == 0) Right(i) else Right("not even")
    ))
    println("traverse again:" + Either.traverse(Nil)(
      i => Right(i)
    ))

    def f2[EE <: Int, AA <: Int](b: => Either[EE, AA]): Unit = {
      val ret = for {
        a <- b
        //println(a)
      } yield a

      ret match {
        case Left(x) => println(x)
        case Right(x) => println(x + 1)
      }
    }


    //val e1 = Left(1,"A")
    val e1 = Right(1)
    println(f2(e1))

    println("sequence:" + Either.sequence(List(Right(1),Right(2),Right(3),Right(4))))
  }
}
