import java.io.File
import com.typesafe.config.{ Config, ConfigFactory }

object Client extends App  {
  val config: Config = ConfigFactory.load()
  val systemName: String = config.getString("remoteSystem.name")
  for (i <- 1 to 5) {
    val name: String = config.getString(s"remoteSystem.actor$i.name")
    val ip: String = config.getString(s"remoteSystem.actor$i.ip")
    val port: String = config.getString(s"remoteSystem.actor$i.port")
    println(s"actor: akka.tcp://$systemName@$ip:$port/user/$name")
  }
}

