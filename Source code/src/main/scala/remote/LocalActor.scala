package remote

import akka.actor._

object Local extends App {

  implicit val system: ActorSystem = ActorSystem("LocalSystem")
  val localActor = system.actorOf(Props[LocalActor], name = "remote.LocalActor")  // the local actor
  localActor ! "START"                                                     // start the action

}

class LocalActor extends Actor {

  val remote: ActorSelection = context.actorSelection("akka.tcp://HelloRemoteSystem@127.0.0.1:5150/user/remote.RemoteActor")
  var counter = 0

  def receive: PartialFunction[Any, Unit] = {
    case "START" =>
      remote ! "Hello from the remote.LocalActor"
    case msg: String =>
      println(s"remote.LocalActor received message: '$msg'")
      if (counter < 5) {
        sender ! "Hello back to you"
        counter += 1
      }
  }
}
