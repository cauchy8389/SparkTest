package functionalprogramming.laziness

object LazyObj {
  def main(args: Array[String]): Unit = {
    println(Stream.cons(1, Empty))
    println(Stream.empty)
    println(Stream.ones)
    println(Stream.constant(1))
    println(Stream.from(5).take(5).append(Stream.fibs).take(20).toList)
    println(Stream.from(5).append(Stream.ones).take(10).toList)

    println("fibs: ")
    Stream.fibs.take(20).toList.foreach(a => println(a))

    println(Stream.unfold(3)((i) => Some((i,i*2))).take(5).toListFast)
    println(Stream.unfold(3)((i) => Some((i,i*2))).headOption.get)

    Stream.fibs.tails.take(3).toList.foreach( x=> println(x.take(5).toList))
  }
}
