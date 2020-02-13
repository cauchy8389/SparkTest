// src/main/scala/progscala3/implicits/custom-string-interpolator.sc

object Interpolators {
  implicit class mapForStringContext(val sc: StringContext) {  // <1>
    def map(values: String*): Map[String, String] = {          // <2>
      val keyRE = """^[\s{,]*(\S+):\s*""".r                    // <3>
      println(sc.parts(0))
      println(sc.parts(1))
      println(sc.parts(2))
      println(sc.parts.size)
      val keys = sc.parts map {                                // <4>
        case keyRE(key) => key
        case str => str
      }
      println(keys)
      val kvs = keys zip values                                // <5>
      println(kvs)
      kvs.toMap                                                // <6>
    }
  }
}

import Interpolators._

val name = "Dean Wampler"
val book = "Programming Scala, Third Edition"

val map1 = map"{name: $name, book: $book}"                     // <7>
assert(map1 == Map(
  "name" -> "Dean Wampler", 
  "book" -> "Programming Scala, Third Edition"))

val publisher = "O'Reilly"
val map2 = map"{name: $name, book: $book, publisher: $publisher}"
assert(map2 == Map(
  "name" -> "Dean Wampler", 
  "book" -> "Programming Scala, Third Edition",
  "publisher" -> "O'Reilly"))
