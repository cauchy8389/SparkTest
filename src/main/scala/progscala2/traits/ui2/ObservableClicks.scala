// src/main/scala/progscala3/traits/ui2/ObservableClicks.scala
package progscala2.traits.ui2

import progscala2.traits.observer._

trait ObservableClicks extends Clickable with Subject[Clickable] {
  abstract override def click(): String = {        // <1>
    val result = super.click()
    println("ObservableClicks")
    notifyObservers(this)
    result
  }
}
