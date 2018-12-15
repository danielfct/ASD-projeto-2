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
  val sequenceNumber = args(0).toInt
  val hostname = args(1)
  val port = args(2)
  val replicasInfo = args.drop(3)
  val config: Config = buildConfiguration(hostname, port)

  implicit val system: ActorSystem = ActorSystem("Server", config)
  system.actorOf(Application.props(replicasInfo, sequenceNumber), s"application$sequenceNumber")

  def props(replicasInfo: Array[String], sequenceNumber: Int): Props = Props(new Application(replicasInfo, sequenceNumber))
}

class Application(replicasInfo: Array[String], sequenceNumber: Int) extends Actor with ActorLogging {

  val stateMachine: ActorRef = context.actorOf(
    StateMachine.props(sequenceNumber, replicasInfo, self), name = s"stateMachine$sequenceNumber")
  var keyValueStore: Map[String, String] = Map.empty[String, String]  // key -> value
  var clientWrites: Map[String, String] = Map.empty[String, String] // requestId -> result
  var pendingWrites: Set[String] = Set.empty[String] // set of client requestIds
  var leader: ActorRef = _
  val r = new Random

  private def logInfo(msg: String): Unit = {
    log.info(s"\nApplication layer: $msg")
  }

  override def receive(): PartialFunction[Any, Unit] = {

    case Read(key: String) =>
      this.logInfo(s"read key=$key value=${keyValueStore.get(key)}")
      sender ! Response(result = keyValueStore.get(key))

    case Write(key: String, value: String, timestamp: Long) =>
      this.logInfo(s"write key=$key timestamp=$timestamp")
      val requestId: String = s"${sender.path}_$timestamp"
      if (clientWrites.contains(requestId)) {
        sender ! Response(result = clientWrites.get(requestId))
      } else if (leader != null) {
        pendingWrites += requestId
        if (self == leader) {
          stateMachine ! WriteValue(key, value, requestId)
        } else {
          leader forward Write(key, value, timestamp)
        }
      }

    case WriteResponse(operation: WriteOperation) =>
      this.logInfo(s"got write response $operation")
      val key: String = operation.key
      val value: String = operation.value
      val requestId: String = operation.requestId
      val result = keyValueStore.get(key)
      val clientInfo: Array[String] = requestId.split("_")
      clientWrites = clientWrites.filterKeys(k => !k.contains(clientInfo(0)))
      clientWrites += (requestId -> value)
      keyValueStore += (key -> value)
      if (pendingWrites.contains(requestId)) {
        this.logInfo(s"replying to the client $operation")
        pendingWrites -= requestId
        val client = context.actorSelection(s"${clientInfo(0)}")
        client ! Response(result)
      }
      else {
        this.logInfo(s"not replying to the client $operation")
      }

    case UpdateLeader(newLeader) =>
      this.logInfo(s"update leader $newLeader")
      leader = newLeader
  }

}
