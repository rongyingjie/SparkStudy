
package cn.gxufe.spark.mini.spark

import akka.actor.ActorRef

trait RemoteMessage extends java.io.Serializable



case class Register(host:String , port:Int,uuid:String,cores:Int,memory:Int,workerRef:ActorRef) extends RemoteMessage

case class WorkerInfo(host:String , port:Int,uuid:String,cores:Int,memory:Int,actor:ActorRef,
                      var lastUpdateTime:Long = System.currentTimeMillis() ) extends RemoteMessage

case object HeartbeatToMaster

case object CheckWorkerHeartbeat

// master to worker
case class LiveMaster(val masterUrl:String) extends RemoteMessage

case class Heartbeat(workerId:String) extends RemoteMessage