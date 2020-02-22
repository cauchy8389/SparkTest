// src/main/scala/progscala3/typesystem/selftype/ButtonSubjectObserver.sc
import progscala2.typesystem.selftype._
import ButtonSubjectObserver._

val buttons = Vector(new ObservableButton("one"), new ObservableButton("two"))
val observer = new ButtonClickObserver

buttons foreach (_.addObserver(observer))
for (i <- 0 to 2) buttons(0).click()
for (i <- 0 to 4) buttons(1).click()

assert(observer.clicks == Map("one" -> 3, "two" -> 5))
