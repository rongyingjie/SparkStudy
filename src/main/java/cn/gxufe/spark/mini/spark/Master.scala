package cn.gxufe.spark.mini.spark


import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

import scala.collection.mutable
import scala.concurrent.duration._

class Master(val host:String,val port:Int) extends Actor with org.apache.spark.Logging {

  val workers: mutable.Map[String,WorkerInfo] = new mutable.HashMap[String,WorkerInfo]()

  override def preStart(): Unit = {
    import this.context.dispatcher
    context.system.scheduler.schedule(0 millis,3 second,this.context.self,CheckWorkerHeartbeat)
  }

  override def receive:Receive = {
    case Heartbeat(uuid) => {
      if(workers.getOrElse(uuid,null) != null){
        logDebug( "Heartbeat host = " + host+",uuid = " + uuid )
        workers(uuid).lastUpdateTime = System.currentTimeMillis()
      } else {
        logError( " uuid =" + uuid +" not worker !!! ")
      }
    }
    case Register(host,port,uuid,cores,memory,workerRef) => {
      if ( ! workers.contains(uuid)  ){
        workers.put(uuid, new WorkerInfo(host,port,uuid,cores,memory,workerRef))
        workerRef ! LiveMaster(s"akka.tcp://Master@$host:$port/user/Master")
        logInfo("register worker uuid =  " + uuid)
      } else {
        logError( "uuid = " + uuid +" has  register !" )
      }
    }
    case CheckWorkerHeartbeat => {
      val currentTimeMillis = System.currentTimeMillis()
      val removeWorker = workers.filter( t => {  currentTimeMillis - t._2.lastUpdateTime >  1000*6 } )
      for( worker <- removeWorker){
        logInfo( "remove worker id = " + worker._1 + " host = " + worker._2.host )
        workers.remove(worker._1)
      }
    }
  }
}

object Master {

  def main(args: Array[String]): Unit = {
    val host="rongyingjie"
    val port=9999
    val str =
      s"""
         |akka.actor.provider="akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname=$host
         |akka.remote.netty.tcp.port=$port
        """.stripMargin
    val config = ConfigFactory.parseString(str)
    val actorSystem = ActorSystem("Master",config)
    val worker = actorSystem.actorOf(Props(new Master(host,port)),"Master")
    worker ! "connect"
    actorSystem.awaitTermination()
  }

}