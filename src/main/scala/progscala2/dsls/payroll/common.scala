// src/main/scala/progscala3/dsls/payroll/common.scala
package progscala2.dsls.payroll

object common {
  trait AllOpChars {
    def == : Int   // $eq$eq  - arbitrary return type
    def >  : Int   // $greater
    def <  : Int   // $less
    def +  : Int   // $plus
    def -  : Int   // $minus
    def *  : Int   // $times
    def /  : Int   // $div
    def \  : Int   // $bslash
    def |  : Int   // $bar
    def !  : Int   // $bang
    def ?  : Int   // $qmark
    def :: : Int   // $colon$colon
    def %  : Int   // $percent
    def ^  : Int   // $up
    def &  : Int   // $amp
    def @@ : Int   // $at$at
    //  def ## : Int   // $hash$hash (there is already a ## method in AnyRef)
    def ~  : Int   // $tilde
  }

  sealed trait Amount { def amount: Double }                         // <1>

  case class Percentage(amount: Double) extends Amount {
    override def toString = s"$amount%"
  }

  case class Dollars(amount: Double) extends Amount {
    override def toString = s"$$$amount"
  }

  implicit class Units(amount: Double) {                             // <2>
    def percent = Percentage(amount)
    def dollars = Dollars(amount)
  }

  case class Deduction(name: String, amount: Amount) {               // <3>
    override def toString = s"$name: $amount"
  }

  case class Deductions(                                             // <4>
    name: String,
    divisorFromAnnualPay: Double = 1.0,
    var deductions: Vector[Deduction] = Vector.empty) {

    def gross(annualSalary: Double): Double =                        // <5>
      annualSalary / divisorFromAnnualPay

    def net(annualSalary: Double): Double = {
      val g = gross(annualSalary)
      (deductions foldLeft g) {
        case (total, Deduction(deduction@_, amount)) => amount match {
          case Percentage(value) => total - (g * value / 100.0)
          case Dollars(value) => total - value
        }
      }
    }

    override def toString =                                          // <6>
      s"$name Deductions:" + deductions.mkString("\n  ", "\n  ", "")
  }
}
