package pt.unl.fct.asd
package server
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSelection, Cancellable, Props}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object Multipaxos {
  def props(stateMachine: ActorRef, replicas: Set[ActorSelection], sequenceNumber: Int, promise: Int): Props = Props(
    new Multipaxos(stateMachine, replicas, sequenceNumber, promise))
}

class Multipaxos(val stateMachine: ActorRef, var replicas: Set[ActorSelection], var sequenceNumber: Int, var promise: Int)
  extends Actor with ActorLogging {

  var majority: Int = Math.ceil((replicas.size + 1.0) / 2.0).toInt
  var currentOp: Operation = _
  var currentSmPos: Int = -1
  var acceptedAcks: Int = 0
  var leader: ActorRef = _
  var accepted = false
  var acceptTimeout: Cancellable = _

  private def isLeader(sender: ActorRef): Boolean =
    sender.path.name.equals(leader.path.name)

  private def logInfo(msg: String): Unit = {
    log.info(s"\n${self.path.name}-$sequenceNumber: $msg")
  }

  override def receive: PartialFunction[Any, Unit] = {

    case Propose(smPos: Int, operation: Operation) => {
      this.logInfo(s"Proposing $operation for position $smPos")
      currentSmPos = smPos
      currentOp = operation
      replicas.foreach(rep => rep ! Accept(sequenceNumber, smPos, operation))
      acceptTimeout = context.system.scheduler.scheduleOnce(5 seconds, self, Restart(sequenceNumber))
    }

    case Accept(sqn: Int, smPos: Int, operation: Operation) =>
      this.logInfo(s"Got accept request from $sender with sequenceNumber $sqn to position $smPos")
      accepted = false
      // TODO: if i think im the leader -> I have to become a normal replica
      if (sqn >= promise || isLeader(sender)) {
        this.logInfo(s"Accepted operation $operation for position $smPos")
        currentSmPos = smPos
        currentOp = operation
        sender ! Accept_OK(sqn, smPos)
      } else {
        this.logInfo(s"Rejected operation $operation from $sender. Promise is $promise, SequenceNumber is $sqn and leader is $leader")
      }

    case Accept_OK(sqn: Int, smPos: Int) =>
      if (sqn == sequenceNumber && smPos == currentSmPos && !accepted) {
        acceptedAcks += 1
        if (acceptedAcks >= majority) {
          accepted = true
          this.logInfo(s"Got accept_ok from majority for position $smPos")
          if (acceptTimeout != null)
            acceptTimeout.cancel()
          replicas.foreach(rep => rep ! Decided(currentSmPos, currentOp))
        }
      }

    case Restart(sqn) => {
      if (sqn == sequenceNumber) {
        this.logInfo(s"Restarted")
        acceptedAcks = 0
        sequenceNumber = sequenceNumber + replicas.size
        stateMachine ! SetSequenceNumber(sequenceNumber)
        self ! Propose(currentSmPos, currentOp)
      }
    }

    case AddReplica(replica) =>
      this.logInfo(s"Added replica $replica")
      replicas += replica
      majority = Math.ceil((replicas.size + 1.0) / 2.0).toInt

    case RemoveReplica(replica) =>
      this.logInfo(s"Removed replica $replica")
      replicas -= replica
      majority = Math.ceil((replicas.size + 1.0) / 2.0).toInt

    case SetPromise(newPromise) =>
      promise = newPromise
      this.logInfo(s"Changed promise to $promise")

    case SetSequenceNumber(sqn) =>
      this.logInfo(s"Changed sequenceNumber to $sqn!")
      sequenceNumber = sqn

    case UpdateLeader(newLeader) =>
      this.logInfo(s"Update leader $newLeader")
      leader = newLeader

  }
}
