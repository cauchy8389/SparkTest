// src/main/scala/progscala3/objectsystem/linearization/ValueClass.scala
package progscala2.objectsystem.linearization

trait M extends Any {
  def m(): String = "M "
}

trait Digitizer extends Any with M {
  override def m(): String = { "Digitizer " + super.m }

  def digits(s: String): String = s.replaceAll("""\D""", "")
}

trait Formatter extends Any with M {   
  override def m(): String = { "Formatter " + super.m }

  def format(areaCode: String, exchange: String, subnumber: String): String =
    s"($areaCode) $exchange-$subnumber"
}

case class USPhoneNumber(s: String) 
    extends AnyVal with Digitizer with Formatter{

  /**
   * Returns "USPhoneNumber Formatter Digitizer M "
   */
  override def m(): String = { "USPhoneNumber " + super.m }
  
  /** 
   * Return the string representation. 
   * For a USPhoneNumber("987-654-3210"), returns "(987) 654-3210"
   */
  override def toString = {
    val digs = digits(s)
    val areaCode = digs.substring(0,3)
    val exchange = digs.substring(3,6)
    val subnumber  = digs.substring(6,10)
    format(areaCode, exchange, subnumber)
  }
}

