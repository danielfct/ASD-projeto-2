import com.typesafe.config.{Config, ConfigFactory}

object Configuration {
  def buildConfiguration(hostname: String, port: String): Config = {
    ConfigFactory.load(ConfigFactory.parseString(s"""
    akka {
      loglevel = "INFO"
      log-config-on-start = "off"
      debug {
        receive = on
      }
      actor {
        provider = "akka.remote.RemoteActorRefProvider"
      }
      remote {
        enabled-transports = ["akka.remote.netty.tcp"]
        netty.tcp {
          hostname = $hostname
          port = $port
        }
        log-sent-messages = on
        log-received-messages = on
      }
    }"""))
  }
}
