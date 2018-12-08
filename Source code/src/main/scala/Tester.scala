import akka.actor.ActorSystem
import akka.actor.ActorRef

object Tester extends App {
  val system = ActorSystem("Tester")
  var actors = Map.empty[Int,ActorRef]
  
  for(i<-1 to 20) {
    val actor = system.actorOf(StateMachine.props(i), "stateMachine"+i)
    actors += i -> actor 
  }
  
  actors.keySet.foreach(k => actors(k) ! Start(actors.values.toSet))
  
  Thread.sleep(60000);
  
  actors(20) ! Kill
  
  Thread.sleep(120000);
  
  actors.values.foreach(a => a ! PrepareDebug)
  
  actors.values.foreach(a => a ! Debug)
}