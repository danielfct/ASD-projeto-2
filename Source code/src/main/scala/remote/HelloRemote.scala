package remote

import akka.actor._

object HelloRemote extends App  {
  val system = ActorSystem("HelloRemoteSystem")
  val remoteActor = system.actorOf(Props[RemoteActor], name = "remote.RemoteActor")
  remoteActor ! "The remote.RemoteActor is alive"
}

class RemoteActor extends Actor {
  def receive: PartialFunction[Any, Unit] = {
    case msg: String =>
      println(s"remote.RemoteActor received message '$msg'")
      sender ! "Hello from the remote.RemoteActor"
  }
}
