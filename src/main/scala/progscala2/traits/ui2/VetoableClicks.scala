// src/main/scala/progscala3/traits/ui2/VetoableClicks.scala
package progscala2.traits.ui2

trait VetoableClicks extends Clickable {                             // <1>
  // Default number of allowed clicks.
  val maxAllowed = 1                                                 // <2>
  private var count = 0

  abstract override def click(): String = {
    println("VetoableClicks")
    if (count < maxAllowed) {                                        // <3>
      count += 1
      println(s"VetoableClicks:${count}")
      super.click()
    } else {
      val result = s"Max allowed clicks exceeded: $maxAllowed"
      println(result)
      result
    }
  }
}
