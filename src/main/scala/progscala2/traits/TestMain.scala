package progscala2.traits

import progscala2.traits.observer.Observer
import progscala2.traits.ui2.{Button, Clickable, ObservableClicks, VetoableClicks}

object TestMain {
  class ClickCountObserver extends Observer[Clickable] {          // <2>
    var count = 0
    def receiveUpdate(state: Clickable): Unit = count += 1
  }

  def main(args: Array[String]): Unit = {
    val button =
      new Button("Click Me!") with ObservableClicks with VetoableClicks {
        override val maxAllowed = 2                                   // <1>
      }

    val bco1 = new ClickCountObserver
    val bco2 = new ClickCountObserver

    button addObserver bco1
    button addObserver bco2

    (1 to 5) foreach (_ => button.click())

    assert(bco1.count == 2, s"bco1.count ${bco1.count} != 2")       // <3>
    assert(bco2.count == 2, s"bco2.count ${bco2.count} != 2")
    println("Success!")
  }

}
