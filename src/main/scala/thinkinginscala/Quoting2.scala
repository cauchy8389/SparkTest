package thinkinginscala

import thinkinginscala.AtomicTest._

object doQuote {
  implicit class AnyName(val s:String)
    extends AnyVal {
    def singleQuote = s"'$s'"
    def doubleQuote = s""""$s""""
  }

  def main(args: Array[String]): Unit = {
    "Single".singleQuote is "'Single'"
    "Double".doubleQuote is "\"Double\""
  }
}
