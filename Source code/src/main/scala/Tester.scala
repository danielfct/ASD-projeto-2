import akka.actor.{ActorRef, ActorSystem, PoisonPill}

object Tester extends App {
  val system = ActorSystem("Tester")

  var actors = Map.empty[Int, ActorRef]
  for (i <- 1 to 10) {
    val actor = system.actorOf(StateMachine.props(i), "stateMachine"+i)
    actors += i -> actor
  }

  actors.keySet.foreach(k => actors(k) ! Start(actors.values.toSet))
  Thread.sleep(60000)
  actors.values.foreach(a => a ! Debug)
  actors(10) ! PoisonPill
  Thread.sleep(120000)
  actors.values.foreach(a => a ! Debug)
  actors.values.foreach(a => a ! PoisonPill)

  system.terminate()
}
