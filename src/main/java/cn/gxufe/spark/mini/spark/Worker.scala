package cn.gxufe.spark.mini.spark


import java.util.UUID

import akka.actor.{Actor, ActorSelection, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._

class Worker(var host:String,val port:Int,val masterUrl:String) extends Actor with org.apache.spark.Logging{

  var master:ActorSelection = _

  var liveMasterUrl:String = _

  val workerId = UUID.randomUUID().toString


  override def preStart(): Unit = {
    master = this.context.actorSelection(masterUrl)
    this.context.self ! new Register(host,port,workerId,4,2048,this.context.self)
  }

  override def receive:Receive = {
    case LiveMaster(liveMasterUrl) => {
      this.liveMasterUrl = liveMasterUrl
      logInfo("liveMasterUrl = " + liveMasterUrl)
      context.system.scheduler.schedule(0 millis,3 seconds,this.context.self,HeartbeatToMaster)(this.context.dispatcher)
    }
    case Register(hos,port,workerId,cores,memory,workerRef) => {
      master ! new Register(hos,port,workerId,cores,memory,workerRef)
    }
    case HeartbeatToMaster => {
      master ! new Heartbeat(workerId)
    }
  }

}

object Worker {

  def main(args: Array[String]): Unit = {
    val host="rongyingjie"
    val port=8888
    val masterUrl = "akka.tcp://Master@rongyingjie:9999/user/Master"
    val str =
      s"""
         |akka.actor.provider="akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname=$host
         |akka.remote.netty.tcp.port=$port
        """.stripMargin
    val config = ConfigFactory.parseString(str)
    val actorSystem = ActorSystem("worker",config)
    actorSystem.actorOf(Props(new Worker(host,port,masterUrl)),"Worker")
    actorSystem.awaitTermination()
  }


}
