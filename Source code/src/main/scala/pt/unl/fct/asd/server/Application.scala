package pt.unl.fct.asd
package server

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSelection, ActorSystem, Props}
import akka.util.Timeout
import com.typesafe.config.Config
import pt.unl.fct.asd.buildConfiguration
import pt.unl.fct.asd.client.Client
import pt.unl.fct.asd.client.Client.args
import pt.unl.fct.asd.server.Application.{replicasInfo, sequenceNumber, system}

import scala.concurrent.duration._
import scala.concurrent.Await
import scala.util.Random

object Application extends App {
  if (args.length < 6) {
    println("Usage: \"sbt runMain Application sequenceNumber ip port replica1 replica2 replica3 [replica4, ...]\"")
    System.exit(1)
  }
  val sequenceNumber = args(0).toInt
  val hostname = args(1)
  val port = args(2)
  val replicasInfo = args.drop(3)
  val config: Config = buildConfiguration(hostname, port)

  implicit val system: ActorSystem = ActorSystem("Server", config)
  val application: ActorRef = system.actorOf(Application.props(replicasInfo), s"application$sequenceNumber")
  system.actorOf(StateMachine.props(sequenceNumber, replicasInfo, application), s"stateMachine$sequenceNumber")

  def props(replicasInfo: Array[String]): Props = Props(new Application(replicasInfo))
}

class Application(replicasInfo: Array[String]) extends Actor with ActorLogging {

  val replicas: Set[ActorSelection] = this.selectReplicas(replicasInfo)
  var keyValueStore: Map[String, String] = Map.empty[String, String]  // key -> value
  var clientWrites: Map[String, String] = Map.empty[String, String] // requestId -> result
  var leader: ActorRef = _
  val r = new Random

  def selectReplicas(replicasInfo: Array[String]): Set[ActorSelection] = {
    var replicas: Set[ActorSelection] = Set.empty[ActorSelection]
    for (i <- replicasInfo.indices) {
      val replicaInfo: String = replicasInfo(i)
      replicas += context.actorSelection(s"akka.tcp://Server@$replicaInfo")
    }
    log.info(s"\nInitial replicas: $replicas")
    replicas
  }

  private def randomReplica(): ActorSelection = {
    val index = r.nextInt(replicas.size)
    replicas.toSeq(index)
  }

  override def receive(): PartialFunction[Any, Unit] = {

    case Read(key: String) =>
      log.info(s"\nApplication layer: read key=$key value=${keyValueStore.get(key)}")
      sender ! Response(result = keyValueStore.get(key))

    case Write(key: String, value: String, timestamp: Long) =>
      log.info(s"\nApplication layer: write key=$key timestamp=$timestamp")
      val requestId: String = s"${sender.path}_$timestamp"
      if (clientWrites.contains(requestId))
        sender ! Response(result = clientWrites.get(requestId))
      else if (leader != null)
        leader ! WriteValue(key, value, requestId)
      else
        randomReplica ! WriteValue(key, value, requestId)

    case WriteResponse(operation: WriteOperation) =>
      log.info(s"\nApplication layer: got write response $operation")
      leader = sender  //TODO atualizar o leader como se atualiza no paxos
      val result = keyValueStore.get(operation.key)
      val clientInfo: Array[String] = operation.requestId.split("_")
      val client = context.actorSelection(s"${clientInfo(0)}")
      if (clientInfo(1).toLong > System.currentTimeMillis()) {
        clientWrites.filterKeys(k => k.contains(clientInfo(0)))
        clientWrites += (operation.requestId -> operation.value)
      }
      keyValueStore += (operation.key -> operation.value)
      client ! Response(result)
  }

}
