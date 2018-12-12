package pt.unl.fct.asd

import akka.actor.{ActorRef, ActorSelection}

package object server {

  final case class SetLeader(sqn: Int, leader: ActorRef)
  final case class Prepare(n: Int)
  final case class Prepare_OK(n: Int, acceptedN: Int)
  final case class SMPropose(op: Operation)
  final case class CopyState(replicas: Set[ActorRef], serviceMap: Map[String,String])
  final case class Propose(N: Int, op: Operation)
  final case class Accept(sqn: Int, N: Int, op: Operation)
  final case class Accept_OK(sqn: Int, N: Int)
  final case class Decided(N: Int, op: Operation)
  final case class AddReplica(rep: ActorSelection)
  final case class RemoveReplica(rep: ActorSelection)
  final case class SetPromise(Promise: Int)
  final case class SetSequenceNumber(sqn: Int)
  final case class Restart(sqn: Int)
  case object PrepareTimeout
  case object SignalLeaderAlive
  case object LeaderHeartbeat
  case object CheckLeaderAlive
  case object Debug

  sealed trait Operation
  case class ReadOperation(key: String, requestId: String) extends Operation
  case class WriteOperation(key: String, value: String, requestId: String) extends Operation
  case class AddReplicaOperation(replica: ActorRef) extends Operation
  case class RemoveReplicaOperation(replica: ActorRef) extends Operation

  final case class Read(key: String)
  final case class Write(key: String, value: String, timestamp: Long)
  final case class Response(result: Option[String])

  final case class WriteValue(key: String, value: String, requestId: String)
  final case class WriteResponse(operation: WriteOperation)

  final case class ReadResponse(result: String, operation: ReadOperation)



}
