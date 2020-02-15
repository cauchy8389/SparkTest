import org.apache.commons.lang3.StringUtils
// src/main/scala/progscala3/forcomps/for-patterns.sc

val ignoreRegex = """^\s*(#.*|\s*)$""".r                             // <1>
val kvRegex = """^\s*([^=]+)\s*=\s*([^#]+)\s*.*$""".r                // <2>

val properties = """# Book properties
|
                    |book.name = Programming Scala, Third Edition # A comment
                    |book.authors = Dean Wampler
                    |book.publisher = O'Reilly
                    |book.publication-year = 2020
                    """.stripMargin                                                   // <3>

val kvPairs1 = for {
  prop <- properties.split("\n") //.withFilter(x => x.startsWith("book"))
//  if !StringUtils.isBlank(prop)
  if ignoreRegex.findFirstIn(prop) == None
//  kvRegex(key, value) = prop
} yield (prop)

println(kvPairs1.size)

kvPairs1.toSeq.foreach( x => {
  if(ignoreRegex.findFirstIn(x) == None)
    println(x + "matches")
  println(x)
})

val kvPairs = for {
  prop <- properties.split("\n")                                     // <4>
  if kvRegex.pattern.matcher(prop).matches                           // <5>
  kvRegex(key, value) = prop                                         // <6>
} yield (key.trim, value.trim)                                       // <7>

println(kvPairs.toSeq)
//assert(kvPairs.toSeq == Seq(
//  ("book.name", "Programming Scala, Third Edition"),
//  ("book.authors", "Dean Wampler"),
//  ("book.publisher", "O'Reilly"),
//  ("book.publication-year", "2020")))
