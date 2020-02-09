// src/main/scala/progscala3/rounding/if.sc

import progscala2.rounding.WeekDay

WeekDay.values foreach { day =>
  if (WeekDay.isWorkingDay(day)) {
    println(s"$day is a working day")
  } else if (day == WeekDay.Sat) {
    println("It's Saturday")
  } else {
    println("It's Sunday")
  }
}
