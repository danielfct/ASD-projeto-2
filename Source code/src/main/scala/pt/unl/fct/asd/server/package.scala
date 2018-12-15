package pt.unl.fct.asd

import akka.actor.{ActorRef, ActorSelection}

import scala.collection.SortedMap

package object server {

  // Leader related messages
  final case class SetLeader(sequenceNumber: Int, leader: Node)
  final case class Node(application: ActorRef, stateMachine: ActorRef, multipaxos: ActorRef)
  final case class UpdateLeader(leader: Node)
  final case object LeaderHeartbeat

  // Prepare related messages
  final case class Prepare(sequencePosition: Int, sequenceNumber: Int)
  final case class Prepare_OK(sequencePosition: Int, sequenceNumber: Int)

  // Propose related messages
  final case class Propose(sequencePosition: Int, operation: List[Operation])
  final case class Accept(sequencePosition: Int, sequenceNumber: Int, operation: List[Operation])
  final case class Accept_OK(sequencePosition: Int, sequenceNumber: Int, operation: List[Operation])
  final case class Decided(sequencePosition: Int, operations: List[Operation])
  final case class RemoveOldReplica(sequencePosition: Int, replica: ActorRef)
  final case class AddNewReplica(sequencePosition: Int, replica: ActorRef)
  final case class SetStateMachineState(sequencePosition: Int, ops: SortedMap[Int, Operation])
  final case class SetMultiPaxosState(sequencePosition: Int, newReplicas: Set[ActorSelection], newPromise: Int)

  final case class Join(contactNode: ActorRef)
  final case object Debug

  sealed trait Operation
  final case class ReadOperation(key: String, requestId: String) extends Operation
  final case class WriteOperation(key: String, value: String, requestId: String) extends Operation
  final case class AddReplicaOperation(replica: ActorRef) extends Operation
  final case class RemoveReplicaOperation(replica: ActorRef) extends Operation

  final case class Read(key: String)
  final case class Write(key: String, value: String, timestamp: Long)
  final case class Response(result: Option[String])

  final case class WriteValue(key: String, value: String, requestId: String)
  final case class WriteResponse(operation: WriteOperation)
  /*  final case class ReadValue(key: String, requestId: String)
    final case class ReadResponse(operation: ReadOperation)*/
  final case class AddReplica(replica: ActorRef)
  final case class RemoveReplica(replica: ActorRef)


}
