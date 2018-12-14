import java.text.SimpleDateFormat
import java.util.Date

import Configuration.buildConfiguration
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSelection, ActorSystem, Cancellable, Props}
import com.typesafe.config.Config

import scala.collection.SortedMap
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random
import scala.util.control.Breaks._

/*object StateMachine extends App {
  if (args.length < 4) {
    println("Usage: \"sbt runMain StateMachine sequenceNumber ip port replica1 [replica2, ...]\"")
    System.exit(1)
  }

  val sequenceNumber = args(0).toInt
  val hostname = args(1)
  val port = args(2)
  val replicasInfo = args.drop(3)
  val config: Config = buildConfiguration(hostname, port)

  implicit val system: ActorSystem = ActorSystem("StateMachineSystem", config)
  val stateMachine = system.actorOf(StateMachine.props(sequenceNumber, replicasInfo), name = s"stateMachine$sequenceNumber")
  stateMachine ! "START"

  def props(sequenceNumber: Int, replicasInfo: Array[String]): Props =
    Props(new StateMachine(sequenceNumber, replicasInfo))
  val PUT = "PUT"
  val ADD_REPLICA = "ADD_REPLICA"
  val REMOVE_REPLICA = "REMOVE_REPLICA"
}*/

object StateMachine {
  def props(sqn: Int): Props = Props(new StateMachine(sqn))

  val PUT = "PUT"
  val ADD_REPLICA = "ADD_REPLICA"
  val REMOVE_REPLICA = "REMOVE_REPLICA"
}

class Operation(var operationType: String)
class ClientOperation(val oType: String, var key: String, var value: Option[String] = None, var reqid: Long)
  extends Operation(oType)
class ReplicaOperation(var oType: String, var replica: ActorRef)
  extends Operation(oType)

class StateMachine(sequenceNumber: Int/*, replicasInfo:Array[String]*/) extends Actor with ActorLogging {
  override def preStart(): Unit = log.info(s"stateMachine$sequenceNumber has started!")
  override def postStop(): Unit = log.info(s"stateMachine$sequenceNumber has stopped!")

  var currentN: Int = 0
  var operationsToExecute: SortedMap[Int, Operation] = SortedMap.empty[Int, Operation]
  var positionOfLastOpExecuted = 0
  var serviceMap: Map[String, String] = Map.empty[String, String]

  var oldSqn: Int = sequenceNumber
  var mySequenceNumber: Int = sequenceNumber
  var replicas = Set.empty[ActorRef] //var replicas: Set[ActorSelection] = this.selectReplicas(replicasInfo)
  var majority: Int = -1 //var majority: Int = Math.ceil((replicas.size + 1.0) / 2.0).toInt
  var currentLeader: ActorRef = _
  var oldLeader: ActorRef = _
  var myPromise: Int = -1
  var prepareAcks: Int = 0

  var monitorLeaderSchedule: Cancellable = _
  var leaderHeartbeatSchedule: Cancellable = _
  var prepareTimeout: Cancellable = _
  var getKeySchedule: Cancellable = _

  var paxos = context.actorOf(Multipaxos.props(self, replicas, mySequenceNumber, myPromise), "multipaxos"+mySequenceNumber)
  var resultsBackup = Map.empty[Long,String]
  var replicaAddedInSmPos = Map.empty[ActorRef,Int] // position in the state machine sequence in which the replica was added

  var leaderLastHeartbeat: Long = 0
  var TTL: Long = 6000 // leader sends heartbeats every 3 seconds

  //self ! Start

  def selectReplicas(replicasInfo: Array[String]): Set[ActorSelection] = {
    var replicas: Set[ActorSelection] = Set.empty[ActorSelection]
    for (i <- replicasInfo.indices) {
      val replicaInfo: String = replicasInfo(i)
      replicas += context.actorSelection(s"akka.tcp://StateMachineSystem@$replicaInfo")
    }
    replicas
  }

  private def addClientOperation(operation: ClientOperation): Unit = {
    log.info("execute operation added a new client op with type: {}, key: {}, value: {}", operation.oType, operation.key, operation.value)
    serviceMap += (operation.key -> operation.value.get)
    resultsBackup += (operation.reqid -> operation.value.get)
  }

  private def addReplica(replica: ActorRef): Unit = {
    log.info(s"execute operation added a new replica: $replica. currentLeader=$currentLeader")
    replicas += replica
    majority = Math.ceil((replicas.size + 1.0) / 2.0).toInt
    paxos ! AddReplica(replica)
    if (currentLeader == self) {
      val smPos = replicaAddedInSmPos.get(replica).get
      replica ! CopyState(replicas, operationsToExecute.filterKeys(_ < smPos), myPromise, currentN)
    }
  }

  private def removeReplica(replica: ActorRef): Unit = {
    log.info(s"execute operation removed a replica: $replica")
    replicas -= replica
    majority = Math.ceil((replicas.size + 1.0) / 2.0).toInt
    if (replica == self) {
      if (monitorLeaderSchedule != null) monitorLeaderSchedule.cancel()
      self ! Join(currentLeader)
    }
    paxos ! RemoveReplica(replica)
  }

  private def overrideLeader(): Unit = {
    Thread.sleep(Random.nextInt(1000))
    prepareAcks = 0
    if (prepareTimeout != null)
      prepareTimeout.cancel()
    replicas.foreach(rep => rep ! Prepare(mySequenceNumber))
    prepareTimeout = context.system.scheduler.scheduleOnce(5 seconds, self, Timeout("PREPARE"))
    log.info(s"${self.path.name} tries to be the leader")
  }
  
  private def doOperation(operation: Operation): Unit = {
    operation.operationType match {
      case StateMachine.PUT =>
        addClientOperation(operation.asInstanceOf[ClientOperation])
      case StateMachine.ADD_REPLICA =>
        addReplica(operation.asInstanceOf[ReplicaOperation].replica)
      case StateMachine.REMOVE_REPLICA =>
         removeReplica(operation.asInstanceOf[ReplicaOperation].replica)
      }
  }

  private def debug(): Unit = {
    val name: String = self.path.name
    println(s"Debug for statemachine $name:")
    println(s"  - OperationsToExecute: $operationsToExecute")
    println(s"  - ServiceMap: $serviceMap")
    println(s"  - CurrentN: $currentN")
    println(s"  - OldSqn: $oldSqn")
    println(s"  - MySequenceNumber: $mySequenceNumber")
    println(s"  - Replicas: $replicas")
    println(s"  - Majority: $majority")
    printf("  - CurrentLeader: %s\n", if (currentLeader == null) "null" else currentLeader.path.name)
    println(s"  - MyPromise: $myPromise")
    println(s"  - PrepareAcks: $prepareAcks")
    println(s"  - LeaderLastHeartbeat: ${new SimpleDateFormat("dd/MM/yyyy hh:mm:ss.SSS").format(new Date(leaderLastHeartbeat))}")
  }

  override def receive: PartialFunction[Any, Unit] = {

    case Start(reps) =>
      replicas = reps
      majority = Math.ceil((replicas.size + 1.0) / 2.0).toInt
      paxos ! SetReplicas(replicas)
      this.overrideLeader()
      
    case Join(contactNode) => contactNode ! AddReplica(self)

    case SetLeader(sqn, leader) =>
      Thread.sleep(Random.nextInt(1000))
      if (sqn >= myPromise) {
        log.info(s"${self.path.name}.$mySequenceNumber changes its leader to ${leader.path.name}.$sqn ($sqn >= $myPromise)")
        if (leaderHeartbeatSchedule != null) {
          leaderHeartbeatSchedule.cancel()
        }
        if (monitorLeaderSchedule != null) {
          monitorLeaderSchedule.cancel()
        }
        if (leader == self) { // self becomes leader
          leaderHeartbeatSchedule = context.system.scheduler.schedule(3 seconds, 3 seconds, self, SignalLeaderAlive)
          mySequenceNumber = 0
          paxos ! SetSequenceNumber(mySequenceNumber)
          if (oldLeader != null && currentLeader != self) {
            context.system.scheduler.scheduleOnce(5 seconds) {
              if (currentLeader == self) {
                self ! SMPropose(new ReplicaOperation(StateMachine.REMOVE_REPLICA, oldLeader))
                oldLeader = null
              }
            }
          }
        }
        else if (currentLeader == self) { // self is no longer leader
          mySequenceNumber = oldSqn
          paxos ! SetSequenceNumber(mySequenceNumber)
        }
        currentLeader = leader
        paxos ! SetLeader(-1,leader)
        myPromise = sqn
        paxos ! SetPromise(myPromise)
        if (currentLeader != self) {
          monitorLeaderSchedule = context.system.scheduler.schedule(3 seconds, 3 seconds, self, CheckLeaderAlive)
          leaderLastHeartbeat = System.currentTimeMillis()
        }
      }
      else {
        log.info(s"${self.path.name}.$mySequenceNumber refuses to change its leader to ${leader.path.name}.$sqn ($sqn < $myPromise)")
      }

    case SignalLeaderAlive =>
      log.info(s"${self.path.name} sent heartbeat")
      if (currentLeader == self) {
        replicas.foreach(rep => if (rep != self) rep ! LeaderHeartbeat)
      }
      else if (leaderHeartbeatSchedule != null) {
        leaderHeartbeatSchedule.cancel()
      }

    case LeaderHeartbeat =>
      log.info(s"${self.path.name} got heartbeat from ${sender.path.name}")
      if (sender == currentLeader) {
        leaderLastHeartbeat = System.currentTimeMillis()
      }

    case CheckLeaderAlive =>
      log.info(s"${self.path.name} checking health of leader ${currentLeader.path.name}")
      if (System.currentTimeMillis() > leaderLastHeartbeat + TTL) {
        monitorLeaderSchedule.cancel()
        oldLeader = currentLeader
        currentLeader = null
        paxos ! SetLeader(-1,null)
        this.overrideLeader()
      }

    case Prepare(n) =>
      Thread.sleep(Random.nextInt(1000))
      if (n > myPromise) {
        log.info(s"${self.path.name}.$mySequenceNumber sends prepare_ok to ${sender.path.name}.$n ($n > $myPromise)")
        myPromise = n
        paxos ! SetPromise(myPromise)
        sender() ! Prepare_OK(n, currentN)
      } else {
        log.info(s"${self.path.name}.$mySequenceNumber rejects prepare from ${sender.path.name}.$n ($n <= $myPromise)")
      }
      if (n > mySequenceNumber && prepareTimeout != null)
        prepareTimeout.cancel()

    case Prepare_OK(n, acceptedN) =>
      Thread.sleep(Random.nextInt(1000))
      if (n == mySequenceNumber) {
        prepareAcks += 1
        log.info(s"${self.path.name}.$mySequenceNumber got prepare_ok from ${sender.path.name}")
        if (prepareAcks >= majority) {
          if (prepareTimeout != null)
            prepareTimeout.cancel()
          log.info(s"${self.path.name}.$mySequenceNumber got majority. I'm the leader now")
          replicas.foreach(rep => rep ! SetLeader(mySequenceNumber, self))
        }
      }

    case Timeout(step) =>
      val old = mySequenceNumber
      mySequenceNumber = mySequenceNumber + replicas.size
      paxos ! SetSequenceNumber(mySequenceNumber)
      oldSqn = mySequenceNumber
      log.info(s"${self.path.name}.$old timed out! New sqn=$mySequenceNumber")
      prepareAcks = 0
      step match {
        case "PREPARE" =>
          this.overrideLeader()
        case _ => log.info(s"Unexpected timeout with step: $step")
      }
      
    case msg @ Put(id, key, value) => {
      if (currentLeader == self) {
        self ! SMPropose(new ClientOperation(StateMachine.PUT, key, Some(value), id))
        sender ! Response(id,serviceMap.get(key).getOrElse("empty"))
      } else currentLeader forward msg
    }
    
    case Get(id, key) => {
      //if (serviceMap.contains(key)) {
        sender ! Response(id, serviceMap.get(key).getOrElse("empty"))
        //if (getKeySchedule != null) 
          //getKeySchedule.cancel()
      //} else getKeySchedule = context.system.scheduler.scheduleOnce(2 seconds, self, Get(key))  
    }
    
    case msg @ AddReplica(rep) => {
      if (currentLeader == self)
        self ! SMPropose(new ReplicaOperation(StateMachine.ADD_REPLICA, rep))
      else currentLeader forward msg
    }

    case SMPropose(operation) => {
      var propose = true
      if(operation.operationType.equals(StateMachine.PUT)) {
        val op = operation.asInstanceOf[ClientOperation]
        val reqid = op.reqid
        if (resultsBackup.contains(reqid)) {
          log.info("Repeated operation detected! reqid={}, key={}, value={}", reqid, op.key, op.value.getOrElse("empty"))
          sender ! Response(reqid, resultsBackup.get(reqid).get)
          propose = false
        } else sender ! Response(reqid, serviceMap.get(op.key).getOrElse("empty"))
      }
      if (propose) {
        log.info(s"stateMachine$mySequenceNumber will send propose($currentN,$operation)")
        paxos ! Propose(currentN, operation)
      }
    }

    case Decided(smPos, operation) =>
      operationsToExecute += smPos -> operation 
      if (operation.operationType.equals(StateMachine.ADD_REPLICA)) {
        val rep = operation.asInstanceOf[ReplicaOperation].replica
        replicaAddedInSmPos += rep -> smPos
      }
      self ! ExecuteOperations
      currentN += 1

    case msg @ Accept(sqn, smPos, op) =>
      paxos forward msg

    case msg @ Accept_OK(sqn, smPos) =>
      paxos forward msg

    case ExecuteOperations => {
      var opsToExecute = operationsToExecute.drop(positionOfLastOpExecuted)
      if (opsToExecute.nonEmpty) {
        var it = opsToExecute.iterator
        var (previous, operation) = it.next()
        doOperation(operation)
        breakable {
          for ((position, operation) <- it) {
            if (position - previous != 1) {
              break
            }
            doOperation(operation)
            positionOfLastOpExecuted = position+1
            previous = position
          }
        }
      }
    }

    case CopyState(reps, opsToExecute, promise, smPos) =>
      replicas = reps
      majority = Math.ceil((replicas.size + 1.0) / 2.0).toInt
      operationsToExecute ++= opsToExecute
      currentLeader = sender
      myPromise = promise
      currentN = smPos
      monitorLeaderSchedule = context.system.scheduler.schedule(3 seconds, 3 seconds, self, CheckLeaderAlive)
      paxos ! SetReplicas(replicas)
      self ! ExecuteOperations
    
    case SetSequenceNumber(sqn) => mySequenceNumber = sqn

    case PrepareDebug =>
      if (monitorLeaderSchedule != null)
        monitorLeaderSchedule.cancel()

    case Debug =>
      log.info(s"\nDebug for ${self.path.name}:\n" +
        s"  - OperationsToExecute: $operationsToExecute\n" +
        s"  - ServiceMap: $serviceMap\n" +
        s"  - CurrentN: $currentN\n" +
        s"  - OldSqn: $oldSqn\n" +
        s"  - MySequenceNumber: $mySequenceNumber\n" +
        s"  - Replicas: ${replicas.map(r => r.path.name)}\n" +
        s"  - Majority: $majority\n" +
        String.format("  - CurrentLeader: %s\n", if (currentLeader == null) "null" else currentLeader.path.name) +
        s"  - MyPromise: $myPromise\n" +
        s"  - PrepareAcks: $prepareAcks\n" +
        s"  - Paxos: $paxos\n" +
        s"  - LeaderLastHeartbeat: ${new SimpleDateFormat("dd/MM/yyyy hh:mm:ss.SSS").format(new Date(leaderLastHeartbeat))}\n")
  }

}
