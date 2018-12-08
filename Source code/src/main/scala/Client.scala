import akka.actor.{Actor, ActorRef, ActorSelection, ActorSystem, Address, Props}
import com.typesafe.config.{Config, ConfigFactory}
import Configuration.buildConfiguration

object Client extends App {
  if (args.length < 3) {
    println("Usage: \"sbt runMain Client ip port replica1 [replica2, ...]\"")
    System.exit(1)
  }

  val hostname = args(0)
  val port = args(1)
  val replicasInfo = args.drop(2)
  val config: Config = buildConfiguration(hostname, port)

  implicit val system: ActorSystem = ActorSystem("ClientSystem", config)
  val client = system.actorOf(Client.props(replicasInfo), name = "client")
  client ! "START"

  def props(replicasInfo: Array[String]): Props = Props(new Client(replicasInfo))
}

class Client(replicasInfo: Array[String]) extends Actor {

  val replicas: Set[ActorSelection] = selectReplicas(replicasInfo)

  def selectReplicas(replicasInfo: Array[String]): Set[ActorSelection] = {
    var replicas: Set[ActorSelection] = Set.empty[ActorSelection]
    for (i <- replicasInfo.indices) {
      val replicaInfo: String = replicasInfo(i)
      replicas += context.actorSelection(s"akka.tcp://StateMachineSystem@$replicaInfo")
    }
    replicas
  }

  override def receive: Receive = {
    case "START" =>

  }
}
