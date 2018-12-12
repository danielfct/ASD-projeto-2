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
  var currentLeader: ActorRef = _
  var accepted = false

  var acceptTimeout: Cancellable = _

  private def isLeader(s: ActorRef, l: ActorRef): Boolean =
    s.path.parent.name.equals(l.path.name)

  override def receive: PartialFunction[Any, Unit] = {

    case Propose(smPos: Int, op: Operation) => {
      log.info(s"\nmultipaxos$sequenceNumber will propose N:$smPos op:$op (myPromise=$promise)")
      currentSmPos = smPos
      currentOp = op
      replicas.foreach(rep => rep ! Accept(sequenceNumber, smPos, op))
      acceptTimeout = context.system.scheduler.scheduleOnce(5 seconds, self, Restart(sequenceNumber))
    }

    case Accept(sqn: Int, smPos: Int, op: Operation) =>
      log.info(s"\nmultipaxos$sequenceNumber got accept N:$smPos op:$op (myPromise=$promise)")
      accepted = false
      // TODO: if i think im the leader -> I have to become a normal replica
      if (sqn >= promise || isLeader(sender,currentLeader)) {
        log.info(s"\nmultipaxos$sequenceNumber accepted!")
        currentSmPos = smPos
        currentOp = op
        sender ! Accept_OK(sqn, smPos)
      } else {
        //sender ! Restart(sqn)
        log.info("multipaxos$mySequenceNumber rejected the accept sender={} leader={}!", sender.path.parent.name, currentLeader.path.name)
      }

    case Accept_OK(sqn: Int, smPos: Int) =>
      if (sqn == sequenceNumber && smPos == currentSmPos && !accepted) {
        acceptedAcks += 1
        if (acceptedAcks >= majority) {
          accepted = true
          log.info("multipaxos{} got accept_ok from majority N:{}", sequenceNumber, smPos)
          if (acceptTimeout != null)
            acceptTimeout.cancel()
          replicas.foreach(rep => rep ! Decided(currentSmPos, currentOp))
        }
      }

    case Restart(sqn) => {
      if (sqn == sequenceNumber) {
        log.info(s"\nmultipaxos$sequenceNumber restarted!")
        acceptedAcks = 0
        sequenceNumber = sequenceNumber + replicas.size
        stateMachine ! SetSequenceNumber(sequenceNumber)
        self ! Propose(currentSmPos, currentOp)
      }
    }

    case AddReplica(rep) =>
      log.info(s"\nmultipaxos$sequenceNumber added replica!")
      replicas += rep
      majority = Math.ceil((replicas.size + 1.0) / 2.0).toInt

    case RemoveReplica(rep) =>
      log.info(s"\nmultipaxos$sequenceNumber removed replica!")
      replicas -= rep
      majority = Math.ceil((replicas.size + 1.0) / 2.0).toInt

    case SetPromise(newPromise) =>
      promise = newPromise
      log.info(s"\nmultipaxos$sequenceNumber changed promise to $promise!")

    case SetSequenceNumber(sqn) =>
      log.info(s"\nmultipaxos$sequenceNumber changed sqn to $sqn!")
      sequenceNumber = sqn

    /*case SetReplicas(reps) =>
      replicas = reps
      majority = Math.ceil((replicas.size + 1.0) / 2.0).toInt
      log.info(s"\nmultipaxos$sequenceNumber changed replicas set!")*/

    case SetLeader(_, leader) => currentLeader = leader

  }
}
