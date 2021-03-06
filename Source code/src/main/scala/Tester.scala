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

  //TEST FOR LEADER ELECTION AND RE-ELECTION
  /*actors.keySet.foreach(k => actors(k) ! Start(actors.values.toSet))
  Thread.sleep(15000)
  actors.values.foreach(a => a ! Debug)
  actors(5) ! PoisonPill
  Thread.sleep(30000)
  actors.values.foreach(a => a ! Debug)
  actors.values.foreach(a => a ! PoisonPill)*/
  
  // TEST FOR PUTS AND GETS
  /*actors.keySet.foreach(k => actors(k) ! Start(actors.values.toSet))
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
  
  println("Checking if other replicas have those values...")
  future = actors(1) ? Get(5,"a")
  result = Await.result(future, timeout.duration).asInstanceOf[Response].result
  println("Result of Get(a) from rep1 is " + result)
  
  future = actors(2) ? Get(5,"a")
  result = Await.result(future, timeout.duration).asInstanceOf[Response].result
  println("Result of Get(a) from rep2 is " + result)
  
  future = actors(3) ? Get(5,"a")
  result = Await.result(future, timeout.duration).asInstanceOf[Response].result
  println("Result of Get(a) from rep3 is " + result)
  
  future = actors(4) ? Get(5,"a")
  result = Await.result(future, timeout.duration).asInstanceOf[Response].result
  println("Result of Get(a) from rep4 is " + result)
  
  future = actors(2) ? Get(5,"c")
  result = Await.result(future, timeout.duration).asInstanceOf[Response].result
  println("Result of Get(c) from rep2 is " + result)
  
  future = actors(3) ? Get(5,"e")
  result = Await.result(future, timeout.duration).asInstanceOf[Response].result
  println("Result of Get(e) from rep3 is " + result)*/
  
  //TEST FOR ADD_REPLICA AND COPY_STATE
  /*actors.keySet.foreach(k => actors(k) ! Start(actors.values.toSet))
  Thread.sleep(15000)
  actors.values.foreach(a => a ! Debug)
  implicit val timeout = AkkaTimeout(5 seconds)
  var future = actors(5) ? Put(1,"a","b")
  var result = Await.result(future, timeout.duration).asInstanceOf[Response].result
  println("Result of Put(1,a,b) is " + result)
  
  future = actors(5) ? Put(2,"c","d")
  result = Await.result(future, timeout.duration).asInstanceOf[Response].result
  println("Result of Put(2,c,d) is " + result)
  
  
  future = actors(5) ? Put(3,"e","f")
  result = Await.result(future, timeout.duration).asInstanceOf[Response].result
  println("Result of Put(3,e,f) is " + result)
  
  Thread.sleep(10000)
  
  actors += 6 -> system.actorOf(StateMachine.props(6), "stateMachine"+6)
  actors(6) ! Join(actors(5))
  
  Thread.sleep(15000)
  
  actors.values.foreach(a => a ! Debug)
  future = actors(6) ? Get(1,"a")
  result = Await.result(future, timeout.duration).asInstanceOf[Response].result
  println("Result of Get(a) from rep6 is " + result)
  future = actors(6) ? Get(2,"c")
  result = Await.result(future, timeout.duration).asInstanceOf[Response].result
  println("Result of Get(c) from rep6 is " + result)
  future = actors(6) ? Get(3,"e")
  result = Await.result(future, timeout.duration).asInstanceOf[Response].result
  println("Result of Get(e) from rep6 is " + result)
  
  future = actors(6) ? Put(4,"g","h")
  result = Await.result(future, timeout.duration).asInstanceOf[Response].result
  println("Result of Put(2,c,d) is " + result)
  
  Thread.sleep(10000)
  
  future = actors(3) ? Get(4,"g")
  result = Await.result(future, timeout.duration).asInstanceOf[Response].result
  println("Result of Get(g) from rep3 is " + result)
  
  println("Testing check of repeated operations...")
  
  future = actors(6) ? Put(2,"c","d")
  result = Await.result(future, timeout.duration).asInstanceOf[Response].result
  println("Result of Put(2,c,d) is " + result)
  
  actors.values.foreach(a => a ! Debug)
  actors.values.foreach(a => a ! PoisonPill)*/
  
  //TEST FOR BATCHING
  actors.keySet.foreach(k => actors(k) ! Start(actors.values.toSet))
  Thread.sleep(15000)
  actors.values.foreach(a => a ! Debug)
  
  actors(5) ! Put(1,"a","b")
  actors(5) ! Put(2,"c","d")
  actors(5) ! Put(3,"e","f")
  actors(5) ! Put(4,"g","h")
  actors(5) ! Put(5,"i","j")
  actors(5) ! Put(6,"k","l")
  
  Thread.sleep(10000)
  
  implicit val timeout = AkkaTimeout(5 seconds)
  var future = actors(5) ? Get(1,"a")
  var result = Await.result(future, timeout.duration).asInstanceOf[Response].result
  println("Result of Get(a) from rep5 is " + result)
  
  future = actors(5) ? Get(2,"c")
  result = Await.result(future, timeout.duration).asInstanceOf[Response].result
  println("Result of Get(c) from rep5 is " + result)
  
  future = actors(5) ? Get(3,"e")
  result = Await.result(future, timeout.duration).asInstanceOf[Response].result
  println("Result of Get(e) from rep5 is " + result)

  future = actors(5) ? Get(4,"g")
  result = Await.result(future, timeout.duration).asInstanceOf[Response].result
  println("Result of Get(g) from rep5 is " + result)

  future = actors(5) ? Get(5,"i")
  result = Await.result(future, timeout.duration).asInstanceOf[Response].result
  println("Result of Get(i) from rep5 is " + result)
  
  future = actors(5) ? Get(6,"k")
  result = Await.result(future, timeout.duration).asInstanceOf[Response].result
  println("Result of Get(k) from rep5 is " + result)
  
  actors(5) ! Put(7,"m","n")
  actors(5) ! Put(8,"o","p")
  
  Thread.sleep(10000)

  future = actors(5) ? Get(6,"k")
  result = Await.result(future, timeout.duration).asInstanceOf[Response].result
  println("Result of Get(k) from rep5 is " + result)
  
  future = actors(5) ? Get(7,"m")
  result = Await.result(future, timeout.duration).asInstanceOf[Response].result
  println("Result of Get(m) from rep5 is " + result)
  
  future = actors(5) ? Get(8,"o")
  result = Await.result(future, timeout.duration).asInstanceOf[Response].result
  println("Result of Get(o) from rep5 is " + result)
  
  actors(5) ! Put(9,"a1","a1")
  actors(5) ! Put(10,"c1","c1")
  actors(5) ! Put(11,"e1","e1")
  actors(5) ! Put(12,"g1","g1")
  actors(5) ! Put(13,"i1","i1")
  actors(5) ! Put(14,"k1","k1")
  actors(5) ! Put(15,"l1","l1")
  
  Thread.sleep(1200)
  
  future = actors(5) ? Get(9,"a1")
  result = Await.result(future, timeout.duration).asInstanceOf[Response].result
  println("Result of Get(a1) from rep5 is " + result)
  
  future = actors(5) ? Get(10,"c1")
  result = Await.result(future, timeout.duration).asInstanceOf[Response].result
  println("Result of Get(c1) from rep5 is " + result)
  
  future = actors(5) ? Get(11,"e1")
  result = Await.result(future, timeout.duration).asInstanceOf[Response].result
  println("Result of Get(e1) from rep5 is " + result)
  
    future = actors(5) ? Get(12,"g1")
  result = Await.result(future, timeout.duration).asInstanceOf[Response].result
  println("Result of Get(g1) from rep5 is " + result)
  
    future = actors(5) ? Get(13,"i1")
  result = Await.result(future, timeout.duration).asInstanceOf[Response].result
  println("Result of Get(i1) from rep5 is " + result)
  
      future = actors(5) ? Get(14,"k1")
  result = Await.result(future, timeout.duration).asInstanceOf[Response].result
  println("Result of Get(k1) from rep5 is " + result)
  
      future = actors(5) ? Get(15,"l1")
  result = Await.result(future, timeout.duration).asInstanceOf[Response].result
  println("Result of Get(l1) from rep5 is " + result)
  
  Thread.sleep(2000)
  
    future = actors(5) ? Get(9,"a1")
  result = Await.result(future, timeout.duration).asInstanceOf[Response].result
  println("Result of Get(a1) from rep5 is " + result)
  
  future = actors(5) ? Get(10,"c1")
  result = Await.result(future, timeout.duration).asInstanceOf[Response].result
  println("Result of Get(c1) from rep5 is " + result)
  
  future = actors(5) ? Get(11,"e1")
  result = Await.result(future, timeout.duration).asInstanceOf[Response].result
  println("Result of Get(e1) from rep5 is " + result)
  
    future = actors(5) ? Get(12,"g1")
  result = Await.result(future, timeout.duration).asInstanceOf[Response].result
  println("Result of Get(g1) from rep5 is " + result)
  
    future = actors(5) ? Get(13,"i1")
  result = Await.result(future, timeout.duration).asInstanceOf[Response].result
  println("Result of Get(i1) from rep5 is " + result)
  
      future = actors(5) ? Get(14,"k1")
  result = Await.result(future, timeout.duration).asInstanceOf[Response].result
  println("Result of Get(k1) from rep5 is " + result)
  
      future = actors(5) ? Get(15,"l1")
  result = Await.result(future, timeout.duration).asInstanceOf[Response].result
  println("Result of Get(l1) from rep5 is " + result)
  
  system.terminate()
}
