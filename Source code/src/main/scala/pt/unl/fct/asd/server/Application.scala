package pt.unl.fct.asd
package server

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSelection, ActorSystem, Props}
import akka.util.Timeout
import com.typesafe.config.Config
import pt.unl.fct.asd.{buildConfiguration, client}
import pt.unl.fct.asd.client.Client
import pt.unl.fct.asd.client.Client.args
import pt.unl.fct.asd.server.Application.{replicasInfo, sequenceNumber, system}

import scala.concurrent.duration._
import scala.concurrent.Await
import scala.util.Random

object Application extends App {
  if (args.length < 4) {
    println("Usage: \"sbt runMain Application sequenceNumber ip port replica1 [replica2, ...]\"")
    System.exit(1)
  }
  val sequenceNumber: Int = args(0).toInt
  val hostname: String = args(1)
  val port: Int = args(2).toInt
  val replicasInfo: Array[String] = args.drop(3)
  val config: Config = buildConfiguration(hostname, port)

  implicit val system: ActorSystem = ActorSystem("Server", config)
  system.actorOf(Application.props(replicasInfo, sequenceNumber), s"application$sequenceNumber")

  def props(replicasInfo: Array[String], sequenceNumber: Int): Props = Props(new Application(replicasInfo, sequenceNumber))
}

class Application(replicasInfo: Array[String], sequenceNumber: Int) extends Actor with ActorLogging {

  val stateMachine: ActorRef = context.actorOf(
    StateMachine.props(self, sequenceNumber, replicasInfo), name = s"stateMachine$sequenceNumber")
  var keyValueStore: Map[String, String] = Map.empty[String, String]  // key value store. key -> value
  var lastClientWrites: Map[String, String] = Map.empty[String, String] // last write of each client. requestId -> result
  var pendingWrites: Set[String] = Set.empty[String] // set of client requestIds pending to be replied
  var leader: ActorRef = _
  val r = new Random

  private def logInfo(msg: String): Unit = {
    log.info(s"\n${self.path.name}: $msg")
  }

  override def receive(): PartialFunction[Any, Unit] = {

    case Read(key: String) =>
      logInfo(s"Read key=$key value=${keyValueStore.get(key)}")
      sender ! Response(result = keyValueStore.get(key))

    case msg @ Write(key: String, value: String, timestamp: Long) =>
      logInfo(s"Write key=$key value=$value timestamp=$timestamp")
      val requestId: String = s"${sender.path}::$timestamp"
      if (lastClientWrites.contains(requestId)) {
        sender ! Response(result = lastClientWrites.get(requestId))
      } else if (leader != null) {
        if (leader == self) {
          pendingWrites += requestId
          stateMachine ! WriteValue(key, value, requestId)
        } else {
          leader forward msg
        }
      }

    case WriteResponse(operation: WriteOperation) =>
      logInfo(s"Got write response $operation")
      val key: String = operation.key
      val value: String = operation.value
      val requestId: String = operation.requestId
      val result = keyValueStore.get(key)
      val clientInfo: Array[String] = requestId.split("::")
      lastClientWrites = lastClientWrites.filterKeys(k => !k.contains(requestId))
      lastClientWrites += (requestId -> value)
      keyValueStore += (key -> value)
      if (pendingWrites.contains(requestId)) {
        logInfo(s"Replying to the client $operation")
        pendingWrites -= requestId
        val client = context.actorSelection(s"${clientInfo(0)}")
        client ! Response(result)
      }
      else {
        logInfo(s"Replicated operation $operation")
      }

    case UpdateLeader(newLeader: Node) =>
      logInfo(s"Updating leader $newLeader")
      leader = newLeader.application

    case Debug =>
      logInfo(s"\n" +
        s"  - StateMachine: $stateMachine\n" +
        s"  - KeyValueStore: $keyValueStore\n" +
        s"  - LastClientWrites: $lastClientWrites\n" +
        s"  - PendingWrites: $pendingWrites\n" +
        s"  - Leader: $leader")
      stateMachine ! Debug

    case msg @ _ =>
      log.error(s"\nGot unexpected message $msg from $sender")

  }

}
