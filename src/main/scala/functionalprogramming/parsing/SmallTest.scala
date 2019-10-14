package functionalprogramming.parsing

class SwingType{
  def  wantLearned(sw : String) = println("兔子已经学会了"+sw)
}

package swimmingPage{
  object swimming{
    implicit def learningType(s : AminalType) = new SwingType  //将转换函数定义在包中
  }
}
class AminalType

object Stringutils {
  implicit class StringImprovement(val s : String){   //隐式类
    def increment = s.map(x => (x +1).toChar)
  }
}

object AminalType extends  App{
  import functionalprogramming.parsing.swimmingPage.swimming._  //使用时显示的导入
  val rabbit = new AminalType
  rabbit.wantLearned("breaststroke")       //蛙泳

  import functionalprogramming.parsing.Stringutils._
  println("mobin".increment)
  println("mobin".map(x => (x +1).toChar))
}
