import akka.actor.{ActorRef, ActorSelection}

final case class Start(initialReplicas: Set[ActorRef])

case object TryToElectMeAsLeader

final case class SetLeader(leadersqn: Int, leader: ActorRef)

case object SignalLeaderAlive

case object LeaderKeepAlive

case object IsLeaderAlive

final case class Prepare(n: Int)

final case class Prepare_OK(n:Int, sqnAcceptedOp: Int, op: Operation)

final case class SMPropose(op: Operation)

final case class CopyState(replicas: Set[ActorSelection], serviceMap: Map[String,String])

final case class Propose(N: Int, op: Operation)

final case class Accept(N: Int, sqn: Int, op: Operation)

final case class Accept_OK(N: Int, sqn: Int)

final case class Decided(N: Int, op: Operation)

case object ExecuteOperations

final case class Timeout(step: String)

final case class AddReplica(rep: ActorSelection)

final case class RemoveReplica(rep: ActorSelection)

final case class SetPromise(Promise: Int)

case object Kill

case object Debug
