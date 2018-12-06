package replication

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor.Cancellable
import scala.util.Random


object StateMachine {
  def props(sqn: Int): Props = Props(new StateMachine(sqn))
  
  val PUT = "PUT"
  val ADD_REPLICA = "ADD_REPLICA"
  val REMOVE_REPLICA = "REMOVE_REPLICA"
}

class Operation(var opType: String)
class ClientOperation(var oType: String, var param1: String, param2: Option[String] = None) extends Operation(oType)
class ReplicaOperation(var oType: String, var param1: ActorRef) extends Operation(oType)

class StateMachine(sequenceNumber: Int) extends Actor with ActorLogging {
  override def preStart(): Unit = log.info(s"multiplaxos-$sequenceNumber has started!")
  override def postStop(): Unit = log.info(s"multipaxos-$sequenceNumber has stopped!")
  
  var mySequenceNumber = sequenceNumber
  var replicas = Set.empty[ActorRef]
  var majority = Int.MaxValue
  var currentLeader: ActorRef = null
  var myPromise = -1
  var sqnOfAcceptedOp = -1
  var acceptedOp = null
  var prepareAcks = List.empty[(Int,Operation)]
  var numAcceptedAcks: Int = 0
  
  var monitorLeaderSchecule: Cancellable = _
  var signalLeaderAliveSchedule: Cancellable = _
  var timeoutSchedule: Cancellable = _
  
  var keepAlive: Long = 0
  var TTL: Long = 3000
  
  override def receive = {
    case Start(rep) => {
      replicas = rep.values.toSet
      majority = Math.ceil((replicas.size + 1.0) / 2.0).toInt
      //self ! TryToElectMeAsLeader
      val maxKey = rep.keys.max
      if (mySequenceNumber == maxKey) {
        currentLeader = self
        mySequenceNumber = 0;
        signalLeaderAliveSchedule = context.system.scheduler.schedule(3 seconds, 1 second, self, SignalLeaderAlive)
      } else self ! SetLeader(rep(maxKey))
    }
    
    case TryToElectMeAsLeader => {
      Thread.sleep(Random.nextInt(1000))
      replicas.foreach(rep => rep ! Prepare(mySequenceNumber))
      timeoutSchedule = context.system.scheduler.scheduleOnce(22 seconds, self, Timeout("PREPARE"))
      log.info(s"Statemachine-$mySequenceNumber tries to be the leader.")
    }
    
    case SetLeader(leader) => {
      Thread.sleep(Random.nextInt(1000))
      log.info(s"Statemachine-$mySequenceNumber will change its leader to $leader")
      if (currentLeader == self && leader != self && signalLeaderAliveSchedule != null)
        signalLeaderAliveSchedule.cancel()
      
      keepAlive = System.currentTimeMillis()
      currentLeader = leader
      
      if (currentLeader != self)
        monitorLeaderSchecule = context.system.scheduler.schedule(3 seconds, 3 seconds, self, IsLeaderAlive)
    }
    
    case SignalLeaderAlive => {
      log.info(s"Statemachine-$mySequenceNumber sent alive signal")
      if (currentLeader == self)
        replicas.foreach(rep => rep ! LeaderKeepAlive)
      else if (signalLeaderAliveSchedule != null) 
        signalLeaderAliveSchedule.cancel()
    }
    
    case LeaderKeepAlive => keepAlive = System.currentTimeMillis()
    
    case IsLeaderAlive => {
      log.info(s"Statemachine-$mySequenceNumber monitors the leader $currentLeader (ignore null)")
      if (currentLeader != null && currentLeader != self && System.currentTimeMillis() > keepAlive + TTL)
        self ! TryToElectMeAsLeader
    }
    
    case Prepare(n) => {
      Thread.sleep(Random.nextInt(1000))
      log.info(s"Statemachine-$mySequenceNumber got prepare from $n")
      if (n > myPromise) {
        myPromise = n
        sender() ! Prepare_OK(n, sqnOfAcceptedOp, acceptedOp)
        log.info(s"Statemachine-$mySequenceNumber sends prepate_ok to $n")
      }
      if (currentLeader == self && n > mySequenceNumber) {
        if (signalLeaderAliveSchedule != null)
          signalLeaderAliveSchedule.cancel()
        currentLeader = null;
        log.info(s"Statemachine-$mySequenceNumber detects leader with higher sqn. Now my currentLeader = null")
      }
      if (n > mySequenceNumber && timeoutSchedule != null)
        timeoutSchedule.cancel()
    }
    
    case Prepare_OK(n, sqnAcceptedOp, op) => {
      Thread.sleep(Random.nextInt(1000))
      if (n == mySequenceNumber) {
        prepareAcks = (n, op) :: prepareAcks
        log.info(s"Statemachine-$mySequenceNumber got prepare_ok")
        if (prepareAcks.size >= majority) {
          if (timeoutSchedule != null )
            timeoutSchedule.cancel()
          log.info(s"Statemachine-$mySequenceNumber got majority. I'm the leader now.")
          mySequenceNumber = 0
          replicas.foreach(rep => rep ! SetLeader(self))
          /*if (currentLeader != null) {
            val op = new ReplicaOperation(StateMachine.REMOVE_REPLICA, currentLeader)
            self ! Propose(op)
          }*/
          currentLeader = self
          signalLeaderAliveSchedule = context.system.scheduler.schedule(3 seconds, 1 second, self, SignalLeaderAlive)
        }
      }
    }
    
    case Timeout(step) => {
      val old = mySequenceNumber
      mySequenceNumber = mySequenceNumber + replicas.size
      log.info(s"Statemachine-$old timed out! New sqn=$mySequenceNumber")
      prepareAcks = List.empty[(Int,Operation)]
      numAcceptedAcks = 0
      step match {
        case "PREPARE" => self ! TryToElectMeAsLeader
        case "PROPOSE"=> // TODO
        case _ => log.info(s"Unexpected timeout with step: $step")
      }
    }
    
    case Kill => context.stop(self)
    
    case Debug => log.info("my leader is {}", currentLeader); context.stop(self)
    
  }
}