package pt.unl.fct.asd.client

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.Config
import pt.unl.fct.asd.buildConfiguration

object Tester extends App {
  if (args.length < 7) {
    println("Usage: \"sbt runMain Tester initialDelay numberOfClients numberOfOperations percentageOfWrites ip port replica1 [replica2, ...]\"")
    System.exit(1)
  }
  val initialDelay: Long = args(0).toLong
  val numberOfClients: Int = args(1).toInt
  val numberOfOperations: Int = args(2).toInt
  val percentageOfWrites: Int = args(3).toInt
  if (percentageOfWrites < 0 || percentageOfWrites > 100) {
    println("Percentage of writes must be between 0 and 100")
    System.exit(1)
  }
  val hostname: String = args(4)
  val port: Int = args(5).toInt
  val replicasInfo: Array[String] = args.drop(6)
  val config: Config = buildConfiguration(hostname, port)

  println(s"Sleeping for $initialDelay milliseconds")
  Thread.sleep(initialDelay)

  implicit val system: ActorSystem = ActorSystem("Client", config)
  for (i <- 1 to numberOfClients) {
    val client = system.actorOf(Client.props(numberOfOperations, percentageOfWrites, replicasInfo), s"client$i")
    client ! "START"
  }

}
