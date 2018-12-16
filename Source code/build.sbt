name := "ASD Projeto 2"

version := "0.1"

scalaVersion := "2.12.7"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.5.18",
  "com.typesafe.akka" %% "akka-remote" % "2.5.18"
)
