package replication

import akka.actor.ActorRef

final case class Start(initialReplicas: Map[Int,ActorRef])

final case object TryToElectMeAsLeader

final case class SetLeader(leader: ActorRef)

final case object SignalLeaderAlive

final case object LeaderKeepAlive

final case object IsLeaderAlive

final case class Prepare(n: Int)

final case class Prepare_OK(n:Int, sqnAcceptedOp: Int, op: Operation)

final case class Timeout(step: String)

final case object Kill

final case object Debug