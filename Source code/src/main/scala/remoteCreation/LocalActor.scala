package remoteCreation

import akka.actor._
import akka.actor.{Address, AddressFromURIString, Deploy, Props}
import akka.remote.RemoteScope
import remote.Local.{remoteActor, remoteActor2}

object Local extends App {

  implicit val system: ActorSystem = ActorSystem("LocalSystem")
  val address = Address("akka.tcp", "HelloRemoteSystem", "127.0.0.1", 5151)
  val remoteActor = system.actorOf(Props[RemoteActor].withDeploy(Deploy(scope = RemoteScope(address))))
  remoteActor ! "The remote.RemoteActor is alive"
  println(s"remoteActor $remoteActor")

  val remoteActor2 = system.actorOf(Props[RemoteActor].withDeploy(Deploy(scope = RemoteScope(address))))
  remoteActor2 ! "The remote.RemoteActor is alive"
  println(s"remoteActor $remoteActor2")

  val localActor = system.actorOf(Props[LocalActor], name = "remote.LocalActor")  // the local actor
  localActor ! "START"                                                     // start the action
}

class LocalActor extends Actor {

  val remote: ActorSelection = context.actorSelection(remoteActor.path)
  val remote2: ActorSelection = context.actorSelection(remoteActor2.path)
  val remote3: ActorSelection = context.actorSelection("akka.tcp://HelloRemoteSystem@127.0.0.1:5151/user/remote.RemoteActor")
  var counter = 0

  def receive: PartialFunction[Any, Unit] = {
    case "START" =>
      remote ! "Hello from the remote.LocalActor1"
      remote2 ! "Hello from the remote.LocalActor2"
      remote3 ! "Hello from the remote.LocalActor3"
    case msg: String =>
      println(s"remote.LocalActor received message: '$msg' from $sender")
      if (counter < 5) {
        sender ! "Hello back to you"
        counter += 1
      }
  }
}
