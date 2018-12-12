import akka.actor.{ActorRef, ActorSelection}

final case class Start(initialReplicas: Set[ActorRef])

case object TryToElectMeAsLeader

final case class SetLeader(sqn: Int, leader: ActorRef)

case object SignalLeaderAlive

case class LeaderHeartbeat()

case object CheckLeaderAlive

final case class Prepare(n: Int)

final case class Prepare_OK(n: Int, acceptedN: Int)

final case class SMPropose(op: Operation)

final case class CopyState(replicas: Set[ActorRef], serviceMap: Map[String,String])

final case class Propose(N: Int, op: Operation)

final case class Accept(sqn: Int, N: Int, op: Operation)

final case class Accept_OK(sqn: Int, N: Int)

final case class Decided(N: Int, op: Operation)

case object ExecuteOperations

final case class Timeout(step: String)

final case class AddReplica(rep: ActorRef)

final case class RemoveReplica(rep: ActorRef)

final case class SetPromise(Promise: Int)

final case class SetSequenceNumber(sqn: Int)

final case class SetReplicas(replicas: Set[ActorRef])

final case class Restart(sqn: Int)

final case class Response(reqid: Long, result: String)

final case class Put(id: Long, key: String, value: String)

final case class Get(id: Long, key: String)

case object Debug

case object PrepareDebug
