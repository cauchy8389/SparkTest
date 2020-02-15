// src/main/scala/progscala3/fp/datastructs/fibonacci.sc
import scala.math.BigInt

//2.12
//val fibs: LazyList[BigInt] =
//  BigInt(0) #:: BigInt(1) #:: fibs.zip(fibs.tail).map (n => n._1 + n._2)
//
//// Must convert to an "eager" sequence to see the values:
//assert(fibs.take(10).force == LazyList(0, 1, 1, 2, 3, 5, 8, 13, 21, 34))

//2.11
val fibs: Stream[BigInt] =
  BigInt(0) #:: BigInt(1) #:: fibs.zip(fibs.tail).map (n => {
    println(n + "=" + (n._1 + n._2))
    n._1 + n._2
  })


println(fibs.take(2).force)
println(fibs.take(10).force)
