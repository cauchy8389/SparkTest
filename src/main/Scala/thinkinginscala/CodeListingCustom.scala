// CodeListingCustom.scala
package thinkinginscala

import java.io.FileNotFoundException
import util.Success

object CodeListingCustom {
  def apply(name:String) =
    try {
      Success(new CodeListing(name))
    } catch {
      case _:FileNotFoundException =>
        Fail(s"File Not Found: $name")
      case _:NullPointerException =>
        Fail("Error: Null file name")
      case e:ExtensionException =>
        Fail(e.getMessage)
    }

  def listing(name:String) =
    CodeListingCustom(name).recover{
      case e => Vector(e.toString)
    }.get

  def main(args: Array[String]): Unit = {
    new CodeListingTester(listing)

  }
}
