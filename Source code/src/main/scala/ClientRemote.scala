import java.io.File
import com.typesafe.config.{ Config, ConfigFactory }
import akka.actor._
import com.typesafe.config.ConfigFactory
import remote.RemoteActor

object ClientRemote extends App  {

  implicit val system: ActorSystem = ActorSystem("Client")
  val localActor = system.actorOf(Props[ClientRemote], name = "client")  // the local actor
  localActor ! "START"

}

class ClientRemote extends Actor {

  val config: Config = ConfigFactory.load()
  val systemName: String = config.getString("remoteSystem.name")
  val remotes: Array[ActorSelection] = Array.ofDim[ActorSelection](5)
  for (i <- 0 to 4) {
    val name: String = config.getString(s"remoteSystem.actor$i.name")
    val ip: String = config.getString(s"remoteSystem.actor$i.ip")
    val port: String = config.getString(s"remoteSystem.actor$i.port")
    remotes(i) = context.actorSelection(s"akka.tcp://$systemName@$ip:$port/user/$name")
  }

  var counter = 0
  def receive: PartialFunction[Any, Unit] = {
    case "START" =>
      remotes(0) ! "Hello from the client"
    case msg: String =>
      println(s"client received message: '$msg'")
      if (counter < 5) {
        sender ! "Hello back to you"
        counter += 1
      }
  }
}
