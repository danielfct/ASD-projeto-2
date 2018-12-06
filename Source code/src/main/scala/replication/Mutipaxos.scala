package replication

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.Props
import akka.actor.ActorRef
import akka.actor.Cancellable
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object MultiPaxos {
  def props(stateMachine: ActorRef, reps: Set[ActorRef], sqn: Int, promise: Int): Props = Props(
      new MultiPaxos(stateMachine, reps, sqn, promise))
}

class MultiPaxos(stateMachine: ActorRef, reps: Set[ActorRef], sqn: Int, promise: Int) extends Actor with ActorLogging {
  var replicas = reps
  var majority = Math.ceil((replicas.size + 1.0) / 2.0).toInt
  var mySequenceNumber = sqn
  var currentProposedOp: Operation = null
  var currentProposedSmPos = -1
  var acceptedPos = -1
  var sqnOfAcceptedOperation = -1
  var acceptedOperation: Operation = null
  var acceptedAcks = 0
  var myPromise = promise
  
  var timeoutSchedule: Cancellable = _
  
  override def receive = {
    case Propose(smPos: Int, op: Operation) => {
      currentProposedOp = op
      currentProposedSmPos = smPos
      replicas.foreach(rep => Accept(smPos,mySequenceNumber,currentProposedOp))
      timeoutSchedule = context.system.scheduler.scheduleOnce(3 second, self, Timeout)
    }
      
    case Accept(smPos: Int, sqn: Int, op: Operation) => {
      stateMachine ! LeaderKeepAlive
      if (sqn >= myPromise) {
        acceptedPos = smPos
        sqnOfAcceptedOperation = sqn
        acceptedOperation = op
        sender() ! Accept_OK(smPos, sqn)
      }
    }
      
    case Accept_OK(smPos: Int, sqn: Int) => {
      if (sqn == mySequenceNumber) {
        acceptedAcks += 1
        if (acceptedAcks >= majority) {
          if (timeoutSchedule != null)
            timeoutSchedule.cancel()
          replicas.foreach(rep => rep ! Decided(currentProposedSmPos, currentProposedOp))
        }
      }
    }
      
    case Timeout(_) => {
      acceptedAcks = 0
      mySequenceNumber = 10000 // TODO (how to choose a unique sqn?)
      self ! Propose(currentProposedSmPos,currentProposedOp)
    }
    
    case AddReplica(rep) => replicas += rep 
    
    case RemoveReplica(rep) => replicas -= rep
    
    case SetPromise(promise) => myPromise = promise
    
  }
}