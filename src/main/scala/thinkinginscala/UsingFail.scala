// UsingFail.scala
import thinkinginscala._
import thinkinginscala.AtomicTest._
import util.{Try, Success}

object AtomicScala {
  def f(i: Int) =
    if (i < 0)
      Fail(s"Negative value: $i")
    else if (i > 10)
      Fail(s"Value too large: $i")
    else
      Success(i)



  def calc(a: Int, b: String, c: Int) =
    for {
      x <- f(a)
      y <- Try(b.toInt)
      sum = x + y
      z <- f(c)
    } yield sum * z


  def main(args: Array[String]): Unit = {
    f(-1) is "Failure(Negative value: -1)"
    f(7) is "Success(7)"
    f(11) is "Failure(Value too large: 11)"

    calc(10, "11", 7) is "Success(147)"
    calc(15, "11", 7) is
      "Failure(Value too large: 15)"
    calc(10, "dog", 7) is
      "Failure(java.lang." +
        "NumberFormatException: " +
        """For input string: "dog")"""
    calc(10, "11", -1) is
      "Failure(Negative value: -1)"

    val ls = for {
      x <- Some(2)
      y <- Some(5)
    } yield y

    ls.foreach(println)
    println(ls.getOrElse(4))
  }

}
