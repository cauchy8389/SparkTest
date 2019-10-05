package functionalprogramming.parallelism.remote


import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

class ServerActor extends akka.actor.Actor{
  /**
    * receive方法是用来处理客户端发送过来的问题的
    */
  override def receive: Receive = {
    case "start" => println("天猫系统已启动...")

    case ClientMessage(msg) => {
      println(s"收到客户端消息：$msg")
      msg match {
        /**
          * sender()发送端的代理对象， 发送到客户端的mailbox中 -> 客户端的receive
          */
        case "你叫啥" =>
          sender() ! ServerMessage("本宝宝是天猫精灵")
        case "你是男是女" =>
          sender() ! ServerMessage("本宝宝非男非女")
        case "你有男票吗" =>
          sender() ! ServerMessage("本宝宝还小哟")
        case "stop" =>
          context.stop(self) // 停止自己的actorRef
          context.system.terminate() // 关闭ActorSystem，即关闭其内部的线程池（ExcutorService）
          println("天猫系统已停止...")
        case _ =>
          sender() ! ServerMessage("对不起，主人，我不知道你在说什么.......")
      }
    }
  }
}

object ServerActor {
  def main(args: Array[String]): Unit = {
    //定义服务端的ip和端口
    val host = "127.0.0.1"
    val port = 10008
    /**
      * 使用ConfigFactory的parseString方法解析字符串,指定服务端IP和端口
      */
    val config = ConfigFactory.parseString(
      s"""
         |akka.actor.provider="akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname=$host
         |akka.remote.netty.tcp.port=$port
        """.stripMargin)
    /**
      * 将config对象传递给ActorSystem并起名为"Server"，为了是创建服务端工厂对象(ServerActorSystem)。
      */
    val ServerActorSystem = ActorSystem("Server", config)
    /**
      * 通过工厂对象创建服务端的ActorRef
      */
    val serverActorRef = ServerActorSystem.actorOf(Props[ServerActor], "Miao~miao")
    /**
      * 到自己的mailbox -》 receive方法
      */
    serverActorRef ! "start"
  }
}
