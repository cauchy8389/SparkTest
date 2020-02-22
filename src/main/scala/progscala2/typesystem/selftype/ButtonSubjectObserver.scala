// src/main/scala/progscala3/typesystem/selftype/ButtonSubjectObserver.scala
package progscala2.typesystem.selftype

case class Button(label: String) {                                   // <1>
  def click(): Unit = {}
}

object ButtonSubjectObserver extends SubjectObserver {               // <2>
  type S = ObservableButton
  type O = Observer

  class ObservableButton(label: String) extends Button(label) with Subject {
    override def click() = {
      super.click()
      notifyObservers()
    }
  }
}

import ButtonSubjectObserver._

class ButtonClickObserver extends Observer {                   // <3>
 val clicks = new scala.collection.mutable.HashMap[String,Int]()

  def receiveUpdate(button: ObservableButton): Unit = {
    val count = clicks.getOrElse(button.label, 0) + 1
    clicks.update(button.label, count)
  }
}
