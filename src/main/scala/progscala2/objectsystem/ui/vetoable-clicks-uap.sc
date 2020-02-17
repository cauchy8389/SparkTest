// src/main/scala/progscala3/objectsystem/ui/vetoable-clicks-uap.sc
import progscala2.objectsystem.ui.Button
import progscala2.traits.ui2.{Clickable, ObservableClicks}
import progscala2.traits.observer._

trait VetoableClicksUAP extends Clickable {

  def maxAllowed: Int = 1                        // <1>

  private var count = 0

  abstract override def click(): String = {
    println("VetoableClicks")
    if (count < maxAllowed) {
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

val observableButton =
  new Button("Okay") with ObservableClicks with VetoableClicksUAP {
    override val maxAllowed: Int = 2             // <2>
  }

assert(observableButton.maxAllowed == 2,
  s"maxAllowed = ${observableButton.maxAllowed}")

class ClickCountObserver extends Observer[Clickable] {
  var count = 0
  def receiveUpdate(state: Clickable): Unit = count += 1
}

val clickCountObserver = new ClickCountObserver
observableButton.addObserver(clickCountObserver)

val n = 5
for (i <- 1 to n) observableButton.click()

assert(clickCountObserver.count == 2,
  s"count = ${clickCountObserver.count}. Should be != $n")
