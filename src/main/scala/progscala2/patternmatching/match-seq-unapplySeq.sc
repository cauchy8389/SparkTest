import scala.collection.mutable.ArrayBuffer
// src/main/scala/progscala3/patternmatching/match-seq-unapplySeq.sc

val nonEmptyList   = List(1, 2, 3, 4, 5)                             // <1>
val emptyList      = Nil
val nonEmptyMap    = Map("one" -> 1, "two" -> 2, "three" -> 3)

// Process pairs
def windows[T](seq: Seq[T]): String = seq match {
  case Seq(head1, head2, _*) =>                                      // <2>
    s"($head1, $head2), " + windows(seq.tail)                        // <3>
  case Seq(head, _*) => 
    s"($head, _), " + windows(seq.tail)                              // <4>
  case Nil => "Nil"
}

val results = Seq(nonEmptyList, emptyList, nonEmptyMap.toSeq) map {
  seq => windows(seq)
}
assert(results == Seq(
  "(1, 2), (2, 3), (3, 4), (4, 5), (5, _), Nil", 
  "Nil", 
  "((one,1), (two,2)), ((two,2), (three,3)), ((three,3), _), Nil"))

println(nonEmptyList.isInstanceOf[collection.immutable.HashMap[Int,Int]])
println(nonEmptyList.isInstanceOf[Seq[Int]])
println(Seq.unapplySeq(nonEmptyMap.toSeq))

val seq = Seq(1,2,3,4,5)
println(seq.sliding(2).toList)
println(seq.sliding(3,1).toList)
