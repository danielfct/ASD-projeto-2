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
class ClientOperation(var oType: String, var param1: String, var param2: Option[String] = None) extends Operation(oType)
class ReplicaOperation(var oType: String, var param1: ActorRef) extends Operation(oType)

class StateMachine(sequenceNumber: Int) extends Actor with ActorLogging {
  override def preStart(): Unit = log.info(s"multiplaxos-$sequenceNumber has started!")
  override def postStop(): Unit = log.info(s"multipaxos-$sequenceNumber has stopped!")
  
  var currentN = 0
  var operationsToExecute = Map.empty[Int,Operation]
  var canExecute = false
  var serviceMap = Map.empty[String,String]
  
  var mySequenceNumber = sequenceNumber
  var replicas = Set.empty[ActorRef]
  var majority = Int.MaxValue
  var currentLeader: ActorRef = null
  var myPromise = -1
  var sqnOfAcceptedOp = -1
  var acceptedOp = null
  var prepareAcks = List.empty[(Int,Operation)]
  var numAcceptedAcks: Int = 0
  
  var monitorLeaderSchedule: Cancellable = _
  var signalLeaderAliveSchedule: Cancellable = _
  var timeoutSchedule: Cancellable = _
  
  var paxosInstances = Map.empty[Int,ActorRef]
  
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
      } else self ! SetLeader(maxKey,rep(maxKey))
    }
    
    case TryToElectMeAsLeader => {
      Thread.sleep(Random.nextInt(1000))
      replicas.foreach(rep => rep ! Prepare(mySequenceNumber))
      timeoutSchedule = context.system.scheduler.scheduleOnce(22 seconds, self, Timeout("PREPARE"))
      log.info(s"Statemachine-$mySequenceNumber tries to be the leader.")
    }
    
    case SetLeader(leadersqn, leader) => {
      Thread.sleep(Random.nextInt(1000))
      log.info(s"Statemachine-$mySequenceNumber will change its leader to $leader")
      if (currentLeader == self && leader != self && signalLeaderAliveSchedule != null)
        signalLeaderAliveSchedule.cancel()
      
      keepAlive = System.currentTimeMillis()
      currentLeader = leader
      myPromise = leadersqn
      
      if (currentLeader != self)
        monitorLeaderSchedule = context.system.scheduler.schedule(3 seconds, 3 seconds, self, IsLeaderAlive)
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
      if (currentLeader != null && currentLeader != self && System.currentTimeMillis() > keepAlive + TTL) {
        monitorLeaderSchedule.cancel()
        myPromise = -1
        self ! TryToElectMeAsLeader
      }
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
          replicas.foreach(rep => rep ! SetLeader(mySequenceNumber, self))
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
      mySequenceNumber = mySequenceNumber + replicas.size // TODO: (how to choose a unique sqn?)
      log.info(s"Statemachine-$old timed out! New sqn=$mySequenceNumber")
      prepareAcks = List.empty[(Int,Operation)]
      numAcceptedAcks = 0
      step match {
        case "PREPARE" => self ! TryToElectMeAsLeader
        case "PROPOSE"=> // TODO
        case _ => log.info(s"Unexpected timeout with step: $step")
      }
    }
    
    /*def getPaxosInstance(id: Int): ActorRef = {
      var multipaxos = paxosInstances(id)
      if (multipaxos == null) {
        val multipaxosid = mySequenceNumber+1;
        multipaxos = context.actorOf(MultiPaxos.props(self,replicas,multipaxosid,myPromise), 
            s"multipaxos-$multipaxosid-$id") // TODO: pass unique sqn to multipaxos
        paxosInstances += id -> multipaxos
      }
      return multipaxos
    }*/
    
    case SMPropose(operation) => {
      // TODO: substituir a repetição de código pela getPaxosInstance(id). n sei usar defs nesta merda
      var multipaxos = paxosInstances(currentN)
      if (multipaxos == null) {
        val multipaxosid = mySequenceNumber+1;
        multipaxos = context.actorOf(MultiPaxos.props(self,replicas,multipaxosid,myPromise), 
            s"multipaxos-$multipaxosid-$currentN") // TODO: pass unique sqn to multipaxos
        paxosInstances += currentN -> multipaxos
      }
      multipaxos ! Propose(currentN, operation)
      currentN += 1
    }
    
    case Decided(smPos, operation) => {
      operationsToExecute += smPos -> operation
    }
    
    case Accept(smPos, sqn, op) => {
      // TODO: substituir a repetição de código pela getPaxosInstance(id). n sei usar defs nesta merda
      var multipaxos = paxosInstances(currentN)
      if (multipaxos == null) {
        val multipaxosid = mySequenceNumber+1;
        multipaxos = context.actorOf(MultiPaxos.props(self,replicas,multipaxosid,myPromise), 
            s"multipaxos-$multipaxosid-$currentN") // TODO: pass unique sqn to multipaxos
        paxosInstances += currentN -> multipaxos
      }
      multipaxos ! Accept(smPos, sqn, op)
    }
    
    case Accept_OK(smPos, sqn) => {
      // TODO: substituir a repetição de código pela getPaxosInstance(id). n sei usar defs nesta merda
      var multipaxos = paxosInstances(currentN)
      if (multipaxos == null) {
        val multipaxosid = mySequenceNumber+1;
        multipaxos = context.actorOf(MultiPaxos.props(self,replicas,multipaxosid,myPromise), 
            s"multipaxos-$multipaxosid-$currentN") // TODO: pass unique sqn to multipaxos
        paxosInstances += currentN -> multipaxos
      }
      multipaxos ! Accept_OK(smPos, sqn)
    }
    
    case ExecuteOperations => {
      if (operationsToExecute.keys.size > 0) {
        var it = operationsToExecute.keys.iterator
        var canContinue = true;
        while(it.hasNext && canContinue) {
          val pos = it.next
          val nxtpos = it.next
          if (nxtpos != null && (nxtpos - pos) != 1) {
            canExecute = false
            canContinue = false
          }
        }
        if (canContinue) {
          it = operationsToExecute.keys.iterator
          while(it.hasNext) {
            val pos = it.next
            val operation = operationsToExecute(pos)
            operation.opType match {
              case StateMachine.PUT => {
                val putOp = operation.asInstanceOf[ClientOperation]
                serviceMap += (putOp.param1 -> putOp.param2.get)
              }
              
              case StateMachine.ADD_REPLICA => {
                val addOp = operation.asInstanceOf[ReplicaOperation]
                val rep = addOp.param1
                replicas += rep
                majority = Math.ceil((replicas.size + 1.0) / 2.0).toInt
                paxosInstances.values.foreach(p => p ! AddReplica(rep))
                if (currentLeader == self)
                  rep ! CopyState(replicas, serviceMap)
              }
              
              case StateMachine.REMOVE_REPLICA => {
                val rmvOp = operation.asInstanceOf[ReplicaOperation]
                val rep = rmvOp.param1
                replicas -= rep
                majority = Math.ceil((replicas.size + 1.0) / 2.0).toInt
                paxosInstances.values.foreach(p => p ! RemoveReplica(rep))
              }
            }
            operationsToExecute -= pos
          }
          canExecute = true
        }
        
      }
    }
    
    case CopyState(reps, smap) => {
      replicas = reps
      serviceMap = smap
    }
    
    case Kill => context.stop(self)
    
    case Debug => log.info("my leader is {}", currentLeader); context.stop(self)
    
  }
}