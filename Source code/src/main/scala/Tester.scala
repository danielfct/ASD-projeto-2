import akka.actor.{ActorRef, ActorSystem, PoisonPill}

object Tester extends App {
  val system = ActorSystem("Tester")

  var actors = Map.empty[Int, ActorRef]
  for (i <- 1 to 5) {
    val actor = system.actorOf(StateMachine.props(i), "stateMachine"+i)
    actors += i -> actor
  }

  actors.keySet.foreach(k => actors(k) ! Start(actors.values.toSet))
  Thread.sleep(15000)
  actors.values.foreach(a => a ! Debug)
  actors(5) ! PoisonPill
  Thread.sleep(30000)
  actors.values.foreach(a => a ! Debug)
  actors.values.foreach(a => a ! PoisonPill)

  system.terminate()
}
