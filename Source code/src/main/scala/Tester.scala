import akka.actor.ActorSystem
import akka.actor.ActorRef

object Tester extends App {
  val system = ActorSystem("Tester")
  var actors = Map.empty[Int,ActorRef]
  
  for(i<-1 to 3) {
    val actor = system.actorOf(StateMachine.props(i), "stateMachine"+i)
    actors += i -> actor 
  }
  
  actors.keySet.foreach(k => actors(k) ! Start(actors.values.toSet))
  
  Thread.sleep(10000);
  
  actors(3) ! Kill
  
  Thread.sleep(10000);
  
  actors.values.foreach(a => a ! Debug)
}