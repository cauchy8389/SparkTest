// src/main/scala/progscala3/metaprogramming/func.sc

import scala.reflect.runtime.universe._

class CSuper                { def msuper() = println("CSuper") }
class C      extends CSuper { def m()      = println("C") }
class CSub   extends C      { def msub()   = println("CSub") }

assert(typeOf[C      => C     ] =:= typeOf[C => C] == true)  // <1>
assert(typeOf[CSuper => CSub  ] =:= typeOf[C => C] == false) 
assert(typeOf[CSub   => CSuper] =:= typeOf[C => C] == false) 

assert(typeOf[C      => C     ] <:< typeOf[C => C] == true)  // <2>
assert(typeOf[CSuper => CSub  ] <:< typeOf[C => C] == true)  // <3>
assert(typeOf[CSub   => CSuper] <:< typeOf[C => C] == false) // <4>

val c = new C // <2>
def toType[T : TypeTag](t: T): Type = typeOf[T]
val TypeRef(pre, typName, parems) = toType(c)
println((pre, typName, parems))