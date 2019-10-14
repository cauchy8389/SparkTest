package functionalprogramming.parallelism.remote

import akka.actor.{Actor, ActorSelection, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

import scala.io.StdIn

class ClientActor(host: String, port: Int) extends Actor{

  var serverActorRef: ActorSelection = _ // 服务端的代理对象

  // 在receive方法之前调用
  override def preStart(): Unit = {
    // akka.tcp://Server@127.0.0.1:8088
    serverActorRef = context.actorSelection(s"akka.tcp://Server@${host}:${port}/user/Miao~miao")
  }
  // mailbox ->receive
  override def receive: Receive = { // shit
    case "start" => println("2018天猫精灵为您服务！")
    case msg: String => { // shit
      serverActorRef ! ClientMessage(msg) // 把客户端输入的内容发送给 服务端（actorRef）--》服务端的mailbox中 -> 服务端的receive
    }
    case ServerMessage(msg) => println(s"收到服务端消息：$msg")
  }
}

object ClientActor  {
  def main(args: Array[String]): Unit = {

    //指定客户端的IP和端口
    val host = "127.0.0.1"
    val port  = 10009

    //指定服务端的IP和端口
    val serverHost = "127.0.0.1"
    val serverPort = 10008

    /**
      * 使用ConfigFactory的parseString方法解析字符串,指定客户端IP和端口
      */
    val config = ConfigFactory.parseString(
      s"""
         |akka.actor.provider="akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname=$host
         |akka.remote.netty.tcp.port=$port
        """.stripMargin)

    /**
      * 将config对象传递给ActorSystem并起名为"Server"，为了是创建客户端工厂对象(clientActorSystem)。
      */
    val clientActorSystem = ActorSystem("client", config)

    // 创建dispatch | mailbox
    val clientActorRef = clientActorSystem.actorOf(Props(new ClientActor(serverHost, serverPort.toInt)), "Client")

    clientActorRef ! "start" // 自己给自己发送了一条消息 到自己的mailbox => receive

    /**
      * 接受用户的输入信息并传送给服务端
      */
    while (true) {
      Thread.sleep(500)
      /**
        * StdIn.readLine方法是同步阻塞的
        */
      val question = StdIn.readLine("请问有什么我可以帮你的吗?>>>")
      clientActorRef ! question
      if (question.equals("stop")){
        Thread.sleep(500)
        println("程序即将结束")
        System.exit(0)
      }
    }
  }
}
