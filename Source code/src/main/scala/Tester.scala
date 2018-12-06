import akka.actor.ActorSystem
import akka.actor.ActorRef

import replication._

object Tester extends App {
  val system = ActorSystem("Tester")
  var actors = Map.empty[Int,ActorRef]
  
  for(i<-1 to 5) {
    val actor = system.actorOf(StateMachine.props(i), "statemachine-"+i)
    actors += i -> actor 
  }
  
  actors.keySet.foreach(k => actors(k) ! Start(actors))
  
  /*Thread.sleep(10000);
  
  actors(5) ! Kill
  
  Thread.sleep(10000)
  
  actors.values.foreach(a => a ! Debug)*/
}