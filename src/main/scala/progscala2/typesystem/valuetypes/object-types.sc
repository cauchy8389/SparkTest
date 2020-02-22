import progscala2.typesystem.valuetypes.Service1
// src/main/scala/progscala3/typesystem/valuetypes/object-types.sc

case object Foo { override def toString = "Foo says Hello!" }

def fooString(foo: Foo.type) = s"Foo.type: $foo"

assert(fooString(Foo) == "Foo.type: Foo says Hello!")


case class C(s: String)
val c1 = C("c1")
println(c1)
val c1b: c1.type = c1
println(c1b)
// val c1b: c1.type = C("c1b")            // Error

val s11 = new Service1
println(s11.logger)
val l11: s11.logger.type = s11.logger

val xll1:  Int Either Double  Either String  = Left(Left(1))
val xll2: (Int Either Double) Either String  = Left(Left(1))

val xlr1:  Int Either Double  Either String  = Left(Right(3.14))
val xlr2: (Int Either Double) Either String  = Left(Right(3.14))

val xr1:   Int Either Double  Either String  = Right("foo")
val xr2:  (Int Either Double) Either String  = Right("foo")

val xl:   Int Either (Double Either String)  = Left(1)
val xrl:  Int Either (Double Either String)  = Right(Left(3.14))
val xrr:  Int Either (Double Either String)  = Right(Right("bar"))
