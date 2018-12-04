import akka.actor.{Actor, ActorSystem, Props}
import remote.RemoteActor

object RemoteTester extends App  {
  val system = ActorSystem("MultiPaxosRemoteSystem")
  val id: Long = 0
  val remoteActor = system.actorOf(MultiPaxos.props(id), name = s"multipaxos$id")
  remoteActor ! s"multipaxos$id actor is alive"
}

object MultiPaxos {
  def props(id: Long): Props = Props(new MultiPaxos(id))
}

class MultiPaxos(id: Long) extends Actor {

  val name: String = s"multipaxos$id"

  def receive: PartialFunction[Any, Unit] = {
    case msg: String =>
      println(s"$name received message '$msg'")
      sender ! s"Hello from $name"
  }

}
