import akka.actor.{ActorRef, ActorSystem, PoisonPill}
import akka.actor.Actor
import akka.util.{Timeout => AkkaTimeout}
import scala.concurrent.duration._
import scala.concurrent.Await
import akka.pattern.ask

object Tester extends App {
  val system = ActorSystem("Tester")

  var actors = Map.empty[Int, ActorRef]
  for (i <- 1 to 5) {
    val actor = system.actorOf(StateMachine.props(i), "stateMachine"+i)
    actors += i -> actor
  }

  /*actors.keySet.foreach(k => actors(k) ! Start(actors.values.toSet))
  Thread.sleep(15000)
  actors.values.foreach(a => a ! Debug)
  actors(5) ! PoisonPill
  Thread.sleep(30000)
  actors.values.foreach(a => a ! Debug)
  actors.values.foreach(a => a ! PoisonPill)*/
  
  actors.keySet.foreach(k => actors(k) ! Start(actors.values.toSet))
  Thread.sleep(15000)
  actors.values.foreach(a => a ! Debug)
  implicit val timeout = AkkaTimeout(5 seconds)
  var future = actors(5) ? Put(1,"a","b")
  var result = Await.result(future, timeout.duration).asInstanceOf[Response].result
  println("Result of Put(1,a,b) is " + result)
  Thread.sleep(10000)
  future = actors(5) ? Get(2,"a")
  result = Await.result(future, timeout.duration).asInstanceOf[Response].result
  println("Result of Get(a) is " + result)
  
  future = actors(5) ? Put(3,"c","d")
  result = Await.result(future, timeout.duration).asInstanceOf[Response].result
  println("Result of Put(3,c,d) is " + result)
  Thread.sleep(10000)
  future = actors(5) ? Get(3,"c")
  result = Await.result(future, timeout.duration).asInstanceOf[Response].result
  println("Result of Get(c) is " + result)
  
  future = actors(5) ? Put(4,"e","f")
  result = Await.result(future, timeout.duration).asInstanceOf[Response].result
  println("Result of Put(4,e,f) is " + result)
  Thread.sleep(10000)
  future = actors(5) ? Get(4,"e")
  result = Await.result(future, timeout.duration).asInstanceOf[Response].result
  println("Result of Get(e) is " + result)
  
  println("Checking if value of key 'a' is still stored (should be 'b')...")
  future = actors(5) ? Get(5,"a")
  result = Await.result(future, timeout.duration).asInstanceOf[Response].result
  println("Result of Get(a) is " + result)
  
  println("Checking if state machine detects a repetead operation...")
  future = actors(5) ? Put(3,"c","d")
  result = Await.result(future, timeout.duration).asInstanceOf[Response].result
  println("Result of Put(3,c,d) - repeated - is " + result)

  system.terminate()
}
