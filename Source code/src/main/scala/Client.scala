import java.io.File
import com.typesafe.config.{ Config, ConfigFactory }

object Client extends App  {
  val config: Config = ConfigFactory.load()
  val systemName = config.getString("actorSystem.name")
  println(s"$systemName")
  val ip = config.getString("actorSystem.ip")
  println(s"$ip")
  val port = config.getString("actorSystem.port")
  println(s"$port")
  val actors: Array[String] = config.getString("actorSystem.actors").split(",")
  actors.foreach { println }
}


