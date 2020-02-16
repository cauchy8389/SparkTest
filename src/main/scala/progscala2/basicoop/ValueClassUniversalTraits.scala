// src/main/scala/progscala3/basicoop/ValueClassUniversalTraits.scala
package progscala2.basicoop

trait Digitizer extends Any {
  def digits(s: String): String = s.replaceAll("""\D""", "")         // <1>
}

trait Formatter extends Any {                                        // <2>
  def format(areaCode: String, exchange: String, subnumber: String): String =
    s"($areaCode) $exchange-$subnumber"
}

/**
 * Simple constructor that does not validation of the input string,
 * for simplicity. See `ZipCode` for an example of how this might
 * be done.
 */
case class USPhoneNumberUT(s: String) extends AnyVal
    with Digitizer with Formatter {

  override def toString = {
    val digs = digits(s)
    val areaCode = digs.substring(0,3)
    val exchange = digs.substring(3,6)
    val subnumber  = digs.substring(6,10)
    format(areaCode, exchange, subnumber)                            // <3>
  }
}
