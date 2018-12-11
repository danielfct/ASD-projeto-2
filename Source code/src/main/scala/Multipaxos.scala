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
  var currentOp: Operation = _
  var currentSmPos: Int = -1
  var acceptedAcks: Int = 0
  var myPromise: Int = promise
  var currentLeader: ActorRef = null
  var accepted = false

  var acceptTimeout: Cancellable = _
  
  private def isLeader(s: ActorRef, l: ActorRef): Boolean = 
    s.path.parent.name.equals(l.path.name)

  override def receive: PartialFunction[Any, Unit] = {

    case Propose(smPos: Int, op: Operation) => {
      log.info("multipaxos{} will propose N:{} op:{}. (myPromise={})", mySequenceNumber, smPos, op.operationType, myPromise)
      currentSmPos = smPos
      currentOp = op
      replicas.foreach(rep => rep ! Accept(mySequenceNumber, smPos, op))
      acceptTimeout = context.system.scheduler.scheduleOnce(5 seconds, self, Restart(mySequenceNumber))    
    }
      
    case Accept(sqn: Int, smPos: Int, op: Operation) =>
      log.info("multipaxos{} got accept N:{} op:{}. (myPromise={})", mySequenceNumber, smPos, op.operationType, myPromise)
      accepted = false
      // TODO: if i think im the leader -> I have to become a normal replica
      if (sqn >= myPromise || isLeader(sender,currentLeader)) {
        log.info(s"multipaxos$mySequenceNumber accepted!")
        currentSmPos = smPos
        currentOp = op
        sender ! Accept_OK(sqn, smPos)
      } else {
        //sender ! Restart(sqn)
        log.info("multipaxos$mySequenceNumber rejected the accept sender={} leader={}!", sender.path.parent.name, currentLeader.path.name)
      }

    case Accept_OK(sqn: Int, smPos: Int) =>
      if (sqn == mySequenceNumber && smPos == currentSmPos && !accepted) {
        acceptedAcks += 1
        if (acceptedAcks >= majority) {
          accepted = true
          log.info("multipaxos{} got accept_ok from majority N:{}", mySequenceNumber, smPos)
          if (acceptTimeout != null)
            acceptTimeout.cancel()
          replicas.foreach(rep => rep ! Decided(currentSmPos, currentOp))
        }
      }

    case Restart(sqn) => {
      if (sqn == mySequenceNumber) {
        log.info(s"multipaxos$mySequenceNumber restarted!")
        acceptedAcks = 0
        mySequenceNumber = mySequenceNumber + replicas.size
        stateMachine ! SetSequenceNumber(mySequenceNumber)
        self ! Propose(currentSmPos, currentOp)
      }
    }     

    case AddReplica(rep) => 
      log.info(s"multipaxos$mySequenceNumber added replica!")
      replicas += rep
      majority = Math.ceil((replicas.size + 1.0) / 2.0).toInt

    case RemoveReplica(rep) => 
      log.info(s"multipaxos$mySequenceNumber removed replica!")
      replicas -= rep
      majority = Math.ceil((replicas.size + 1.0) / 2.0).toInt

    case SetPromise(promise) => 
      myPromise = promise
      log.info(s"multipaxos$mySequenceNumber changed promise to $promise!")
    
    case SetSequenceNumber(sqn) => 
      log.info(s"multipaxos$mySequenceNumber changed sqn to $sqn!")
      mySequenceNumber = sqn
          
    case SetReplicas(reps) => 
      replicas = reps
      majority = Math.ceil((replicas.size + 1.0) / 2.0).toInt
      log.info(s"multipaxos$mySequenceNumber changed replicas set!")
      
    case SetLeader(_, leader) => currentLeader = leader

  }
}
