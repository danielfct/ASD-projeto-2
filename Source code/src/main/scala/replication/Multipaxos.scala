package replication

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.Props
import akka.actor.ActorRef
import akka.actor.Cancellable
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object Multipaxos {
  def props(stateMachine: ActorRef, reps: Set[ActorRef], sqn: Int, promise: Int): Props = Props(
    new Multipaxos(stateMachine, reps, sqn, promise))
}

class Multipaxos(stateMachine: ActorRef, reps: Set[ActorRef], sqn: Int, promise: Int) extends Actor with ActorLogging {
  var replicas: Set[ActorRef] = reps
  var majority: Int = Math.ceil((replicas.size + 1.0) / 2.0).toInt
  var mySequenceNumber: Int = sqn
  var currentProposedOp: Operation = _
  var currentProposedSmPos: Int = -1
  var acceptedPos: Int = -1
  var sqnOfAcceptedOperation: Int = -1
  var acceptedOperation: Operation = _
  var acceptedAcks: Int = 0
  var myPromise: Int = promise

  var acceptTimeout: Cancellable = _

  override def receive: PartialFunction[Any, Unit] = {

    case Propose(smPos: Int, op: Operation) =>
      currentProposedSmPos = smPos
      currentProposedOp = op
      replicas.foreach(rep => rep ! Accept(smPos, mySequenceNumber, op))
      acceptTimeout = context.system.scheduler.scheduleOnce(3 second, self, Timeout)

    case Accept(smPos: Int, sqn: Int, op: Operation) =>
      stateMachine ! LeaderKeepAlive
      if (sqn >= myPromise) {
        acceptedPos = smPos
        sqnOfAcceptedOperation = sqn
        acceptedOperation = op
        sender ! Accept_OK(smPos, sqn)
      }

    case Accept_OK(smPos: Int, sqn: Int) =>
      if (sqn == mySequenceNumber) {
        acceptedAcks += 1
        if (acceptedAcks >= majority) {
          if (acceptTimeout != null)
            acceptTimeout.cancel()
          replicas.foreach(rep => rep ! Decided(currentProposedSmPos, currentProposedOp))
        }
      }

    case Timeout(_) =>
      acceptedAcks = 0
      mySequenceNumber = mySequenceNumber + replicas.size
      self ! Propose(currentProposedSmPos, currentProposedOp)

    case AddReplica(rep) => replicas += rep

    case RemoveReplica(rep) => replicas -= rep

    case SetPromise(promise) => myPromise = promise

  }
}
