package pt.unl.fct

import com.typesafe.config.{Config, ConfigFactory}

package object asd {
  def buildConfiguration(hostname: String, port: Int): Config = {
    ConfigFactory.load(ConfigFactory.parseString(s"""
    akka {
      loglevel = "INFO"
      log-config-on-start = "off"
      log-dead-letters-during-shutdown = "off"
      log-dead-letters = 1
      debug {
        receive = on
      }
      actor {
        provider = "akka.remote.RemoteActorRefProvider"
        serializers {
          proto = "akka.remote.serialization.ProtobufSerializer"
        }
        serialization-bindings {
          "pt.unl.fct.asd.client.package" = proto
          "pt.unl.fct.asd.server.package" = proto
        }
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
