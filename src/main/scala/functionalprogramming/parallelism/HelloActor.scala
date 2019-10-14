package functionalprogramming.parallelism

import akka.actor.{Actor, ActorSystem, Props}
import scala.io.StdIn

class HelloActor extends akka.actor.Actor{
  // 重写接受消息的偏函数，其功能是接受消息并处理
  override def receive: Receive = {
    case "你好帅" => println("竟说实话，我喜欢你这种人!")
    case "丑八怪" => {
      println("奶奶的 ！")
      Thread.sleep(1000)
      println("滚犊子 ！")
    }
    case "stop" => {
      context.stop(self) // 停止自己的actorRef
      context.system.terminate() // 关闭ActorSystem，即关闭其内部的线程池（ExcutorService）
    }
  }
}

object HelloActor {
  /**
    * 创建线程池对象MyFactory，用来创建actor的对象的
    */
  private val myFactory = ActorSystem("myFactory")    //里面的"myFactory"参数为线程池的名称
  /**
    *     通过MyFactory.actorOf方法来创建一个actor，注意，Props方法的第一个参数需要传递我们自定义的HelloActor类，
    * 第二个参数是给actor起个名字
    */
  private val helloActorRef = myFactory.actorOf(Props[HelloActor], "helloActor")

  def main(args: Array[String]): Unit = {
    var flag = true
    while(flag){
      /**
        * 接受用户输入的字符串
        */
      print("请输入您想发送的消息:")
      val consoleLine:String = StdIn.readLine()
      /**
        * 使用helloActorRef来给自己发送消息，helloActorRef有一个叫做感叹号("!")的方法来发送消息
        */
      helloActorRef ! consoleLine
      if (consoleLine.equals("stop")){
        flag = false
        println("程序即将结束！")
      }
      /**
        * 为了不让while的运行速度在receive方法之上，我们可以让他休眠0.1秒
        */
      Thread.sleep(100)
    }
  }
}