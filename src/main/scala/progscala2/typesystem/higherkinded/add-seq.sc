// src/main/scala/progscala3/typesystem/higherkinded/add-seq.sc
import progscala2.typesystem.higherkinded.Add              // <1>
import progscala2.typesystem.higherkinded.Add._

def sumSeq[T : Add](seq: Seq[T]): T =                      // <2>
  seq reduce (implicitly[Add[T]].add(_,_))

assert(sumSeq(Vector(1 -> 10, 2 -> 20, 3 -> 30))  == (6 -> 60))
assert(sumSeq(1 to 10)                            == 55)

implicit val a = "test" // define an implicit value of type String
val b = implicitly[String] // search for an implicit value of type String and assign it to b

println(b)