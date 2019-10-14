package functionalprogramming.state

object StateObj {
  def main(args: Array[String]): Unit = {
    println(Candy.simulateMachine(List(Coin,Turn)).run(Machine(true,5,5)))
    println(Candy.update(Coin)(Machine(true,5,5)))

    /**
      * 传入Int,结果翻倍
      */
    val doubleNum :Int => Int  = {num =>
      val numDouble = num * 2
      println(s"double Num ($num * 2 = $numDouble)")
      numDouble
    }

    /**
      *传入Int,结果加一
      */
    val addOne:Int => Int = {num=>
      val sumNum = num + 1
      println(s"add One ($num + 1 = $sumNum)")
      sumNum
    }

    val composeDoubleNumInAddOne = addOne compose doubleNum

    println(composeDoubleNumInAddOne(2))

    def add1(num:Int) = {
      println("num + 2")
      num + 2
    }
    def add2(num:Int) = {
      println("num + 3")
      num + 3
    }
    val addComp = add2 _ compose add1
    println(addComp(1))

    println(Candy.simulateMachine(List(Coin,Turn,Coin)).run(Machine(true,5,5)))

    println("RNG -----------------:" + RNG.Simple(34).nextInt)

    def unit: Testor[Int] = Testor(0)

    case class Testor[+T](value: T) {
      def map[U](f: T => U) : Testor[U] = Testor[U](f(value))

      //def flatMap[U](f: T => Testor[U]): Testor[U] = f(value)

      def foreach[U](f: (T) => U): U = f(value)
    }

    def get[S >: Int]: Testor[S] = Testor(1)
    def set[S >: Int](s: S): Testor[S] = Testor(s)

    //这里的循环 如果没有yield 那就需要Testor里面有foreach
    def doTest[S >: Int]: Testor[S] = for {
      s <- Testor(4)
    } yield s

    def doTest2: Unit = for {
      s <- Testor(999)
    } println(s)

    println(doTest)
    doTest2
  }
}
