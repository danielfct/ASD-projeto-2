import akka.actor.{Actor, ActorLogging, ActorRef, ActorSelection, Cancellable, Props}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object Multipaxos {
  def props(stateMachine: ActorRef, reps: Set[ActorRef], sqn: Int, promise: Int): Props = Props(
    new Multipaxos(stateMachine, reps, sqn, promise))
}

class Multipaxos(stateMachine: ActorRef, reps: Set[ActorRef], sqn: Int, promise: Int) extends Actor with ActorLogging {
  var replicas: Set[ActorRef] = reps
  var majority: Int = Math.ceil((replicas.size + 1.0) / 2.0).toInt
  var mySequenceNumber: Int = sqn
  var currentProposedOp: Operation = _
  var acceptedPos: Int = -1
  var acceptedOperation: Operation = _
  var acceptedAcks: Int = 0
  var myPromise: Int = promise
  var canPropose = false
  var operationsToPropose = List.empty[(Int, Operation)]

  var acceptTimeout: Cancellable = _
  
  context.system.scheduler.scheduleOnce(1 second) {
    doPropose()
  }
    
  def doPropose(): Unit = {
    if (canPropose && operationsToPropose.nonEmpty) {
      canPropose = false
      val head = operationsToPropose.head
      val smPos = head._1
      val operation = head._2
      currentProposedOp = operation
      replicas.foreach(rep => rep ! Accept(mySequenceNumber, smPos, currentProposedOp))
      acceptTimeout = context.system.scheduler.scheduleOnce(5 seconds, self, Restart(mySequenceNumber))
    }
  }

  override def receive: PartialFunction[Any, Unit] = {

    case Propose(smPos: Int, op: Operation) => operationsToPropose = operationsToPropose :+ (smPos, op)
      
    case Accept(sqn: Int, smPos: Int, op: Operation) =>
      stateMachine ! LeaderHeartbeat
      // TODO: if i think im the leader -> I have to become a normal replica
      if (sqn >= myPromise) {
        acceptedPos = smPos
        acceptedOperation = op
        sender ! Accept_OK(sqn)
        stateMachine ! SetState(sqn, smPos, op)
      } // else sender ! Restart(sqn)

    case Accept_OK(sqn: Int) =>
      if (sqn == mySequenceNumber) {
        acceptedAcks += 1
        if (acceptedAcks >= majority) {
          if (acceptTimeout != null)
            acceptTimeout.cancel()
          replicas.foreach(rep => rep ! Decided(acceptedPos, acceptedOperation))
          operationsToPropose = operationsToPropose.drop(1)
          canPropose = true
        }
      }

    case Restart(sqn) => {
      if (sqn == mySequenceNumber) {
        acceptedAcks = 0
        mySequenceNumber = mySequenceNumber + replicas.size
        stateMachine ! SetSequenceNumber(mySequenceNumber)
        canPropose = true
        doPropose()
      }
    }     

    case AddReplica(rep) => 
      replicas += rep
      majority = Math.ceil((replicas.size + 1.0) / 2.0).toInt

    case RemoveReplica(rep) => 
      replicas -= rep
      majority = Math.ceil((replicas.size + 1.0) / 2.0).toInt

    case SetPromise(promise) => myPromise = promise
    
    case SetSequenceNumber(sqn) => mySequenceNumber = sqn
    
    case SetReplicas(reps) => 
      replicas = reps
      majority = Math.ceil((replicas.size + 1.0) / 2.0).toInt

  }
}
