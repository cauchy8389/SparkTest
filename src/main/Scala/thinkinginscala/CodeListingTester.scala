// CodeListingTester.scala
package thinkinginscala

import thinkinginscala.AtomicTest._

class CodeListingTester(
  makeList: String => IndexedSeq[String]) {

  makeList("G:\\CodingResource\\BigData\\atomic-scala-examples-master\\examples-V1.1\\CodeListingTester.scala")(4) is
  "class CodeListingTester("

  makeList("NotAFile.scala")(0) is
  "File Not Found: NotAFile.scala"

  makeList("NotAScalaFile.txt")(0) is
  "NotAScalaFile.txt " +
  "doesn't end with '.scala'"

  makeList(null)(0) is
  "Error: Null file name"

}
