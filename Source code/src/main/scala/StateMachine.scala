import Configuration.buildConfiguration
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSelection, ActorSystem, Cancellable, Props}
import com.typesafe.config.Config

import scala.collection.SortedMap
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
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
class ClientOperation(val oType: String, var key: String, var value: Option[String] = None)
  extends Operation(oType)
class ReplicaOperation(var oType: String, var replica: ActorSelection)
  extends Operation(oType)

class StateMachine(sequenceNumber: Int/*, replicasInfo:Array[String]*/) extends Actor with ActorLogging {
  override def preStart(): Unit = log.info(s"stateMachine$sequenceNumber has started!")
  override def postStop(): Unit = log.info(s"stateMachine$sequenceNumber has stopped!")

  var currentN: Int = 0
  var operationsToExecute: SortedMap[Int, Operation] = SortedMap.empty[Int, Operation]
  var serviceMap: Map[String, String] = Map.empty[String, String]

  var oldSqn: Int = sequenceNumber
  var mySequenceNumber: Int = sequenceNumber
  var replicas = Set.empty[ActorRef] //var replicas: Set[ActorSelection] = this.selectReplicas(replicasInfo)
  var majority: Int = Math.ceil((replicas.size + 1.0) / 2.0).toInt

  var currentLeader: ActorRef = _
  var myPromise: Int = -1
  var sqnOfAcceptedOp: Int = -1
  var acceptedN: Int = -1
  var acceptedOp: Operation = _
  var prepareAcks: List[(Int, Int)] = List.empty[(Int, Int)]

  var monitorLeaderSchedule: Cancellable = _
  var signalLeaderAliveSchedule: Cancellable = _
  var prepareTimeout: Cancellable = _

  var paxosInstances = Map.empty[Int, ActorRef]

  var keepAlive: Long = 0
  var TTL: Long = 12000 //6000 for 10 nodes

  //self ! Start

  def selectReplicas(replicasInfo: Array[String]): Set[ActorSelection] = {
    var replicas: Set[ActorSelection] = Set.empty[ActorSelection]
    for (i <- replicasInfo.indices) {
      val replicaInfo: String = replicasInfo(i)
      replicas += context.actorSelection(s"akka.tcp://StateMachineSystem@$replicaInfo")
    }
    replicas
  }

  /*private def addClientOperation(operation: ClientOperation): Unit = {
    serviceMap += (operation.key -> operation.value.get)
  }

  private def addReplica(replica: ActorSelection): Unit = {
    replicas += replica
    majority = Math.ceil((replicas.size + 1.0) / 2.0).toInt
    paxosInstances.values.foreach(p => p ! AddReplica(replica))
    if (currentLeader == self)
      replica ! CopyState(replicas, serviceMap)
  }

  private def removeReplica(replica: ActorSelection): Unit = {
    replicas -= replica
    majority = Math.ceil((replicas.size + 1.0) / 2.0).toInt
    paxosInstances.values.foreach(p => p ! RemoveReplica(replica))
  }

  private def getPaxosInstance(stateMachinePos: Int): ActorRef = {
    var multipaxos = paxosInstances(stateMachinePos)
    if (multipaxos == null) {
      val multipaxosid = mySequenceNumber + 1 // TODO: pass unique sqn to multipaxos
      multipaxos = context.actorOf(Multipaxos.props(self, replicas, multipaxosid, myPromise),
        s"multipaxos-$multipaxosid-$stateMachinePos")
      paxosInstances += stateMachinePos -> multipaxos
    }
    multipaxos
  }*/

  private def overrideLeader(): Unit = {
    Thread.sleep(Random.nextInt(1000))
    prepareAcks = List.empty[(Int,Int)]
    if (prepareTimeout != null)
      prepareTimeout.cancel()
    replicas.foreach(rep => rep ! Prepare(mySequenceNumber))
    prepareTimeout = context.system.scheduler.scheduleOnce(replicas.size*2 seconds, self, Timeout("PREPARE"))
    log.info(s"Statemachine$mySequenceNumber tries to be the leader.")
  }

  override def receive: PartialFunction[Any, Unit] = {

    case Start(reps) =>
      replicas = reps
      majority = Math.ceil((replicas.size + 1.0) / 2.0).toInt
      this.overrideLeader()

    case SetLeader(sqn, leader) =>
      Thread.sleep(Random.nextInt(1000))
      if (sqn >= myPromise) {
        log.info(s"stateMachine$mySequenceNumber will change its leader to $leader. sqn=$sqn and myPromise was $myPromise")
        if (currentLeader == self && leader != self) {
          if (signalLeaderAliveSchedule != null)
            signalLeaderAliveSchedule.cancel()
          mySequenceNumber = oldSqn
        }
  
        keepAlive = System.currentTimeMillis()
        currentLeader = leader
        myPromise = sqn
  
        if (currentLeader != self)
          monitorLeaderSchedule = context.system.scheduler.schedule(3 seconds, 3 seconds, self, IsLeaderAlive)
      } else log.info(s"stateMachine$mySequenceNumber n mudou merda nenhuma pq sqn=$sqn e myPromise=$myPromise")

    case SignalLeaderAlive =>
      log.info(s"stateMachine$mySequenceNumber sent alive signal")
      if (currentLeader == self)
        replicas.foreach(rep => rep ! LeaderKeepAlive(self))
      else if (signalLeaderAliveSchedule != null)
        signalLeaderAliveSchedule.cancel()

    case LeaderKeepAlive(leader) => if (leader == currentLeader) keepAlive = System.currentTimeMillis()

    case IsLeaderAlive =>
      log.info(s"stateMachine$mySequenceNumber monitors the leader $currentLeader (ignore null)")
      if (currentLeader != null && currentLeader != self && System.currentTimeMillis() > keepAlive + TTL) {
        monitorLeaderSchedule.cancel()
        currentLeader = null
        //myPromise = -1
        this.overrideLeader()
      }

    case Prepare(n) =>
      Thread.sleep(Random.nextInt(1000))
      log.info(s"stateMachine$mySequenceNumber got prepare from $n")
      if (n > myPromise) {
        myPromise = n
        sender() ! Prepare_OK(n, acceptedN, sqnOfAcceptedOp)
        log.info(s"stateMachine$mySequenceNumber sends prepate_ok to $n")
      }
      /*if (currentLeader == self && n > mySequenceNumber) {
        if (signalLeaderAliveSchedule != null)
          signalLeaderAliveSchedule.cancel()
        currentLeader = null
        mySequenceNumber = oldSqn
        log.info(s"stateMachine$mySequenceNumber detects leader with higher sqn. Now my currentLeader = null. Detected n = $n")
      }*/
      if (n > mySequenceNumber && prepareTimeout != null)
        prepareTimeout.cancel()

    case Prepare_OK(n, acceptedN, sqnOfAcceptedOp) =>
      Thread.sleep(Random.nextInt(1000))
      if (n == mySequenceNumber) {
        prepareAcks = (sqnOfAcceptedOp, acceptedN) :: prepareAcks
        log.info(s"stateMachine$mySequenceNumber got prepare_ok")
        if (prepareAcks.size >= majority) {
          currentN = prepareAcks.maxBy(_._1)._2
          if (prepareTimeout != null )
            prepareTimeout.cancel()
          log.info(s"stateMachine$mySequenceNumber got majority. I'm the leader now.")
          replicas.foreach(rep => rep ! SetLeader(mySequenceNumber, self))
          mySequenceNumber = 0
          /*if (currentLeader != null) {
            val op = new ReplicaOperation(StateMachine.REMOVE_REPLICA, currentLeader)
            self ! Propose(op)
          }*/
          currentLeader = self
          signalLeaderAliveSchedule = context.system.scheduler.schedule(3 seconds, 1 second, self, SignalLeaderAlive)
        }
      }

    case Timeout(step) =>
      val old = mySequenceNumber
      mySequenceNumber = mySequenceNumber + replicas.size
      oldSqn = mySequenceNumber
      log.info(s"stateMachine$old timed out! New sqn=$mySequenceNumber")
      prepareAcks = List.empty[(Int,Int)]
      step match {
        case "PREPARE" => this.overrideLeader()
        case "PROPOSE"=> // TODO
        case _ => log.info(s"Unexpected timeout with step: $step")
      }

    /*case SMPropose(operation) =>
      val multipaxos = getPaxosInstance(currentN)
      multipaxos ! Propose(currentN, operation)
      currentN += 1

    case Decided(smPos, operation) =>
      operationsToExecute += smPos -> operation

    case Accept(smPos, sqn, op) =>
      val multipaxos = getPaxosInstance(currentN)
      multipaxos ! Accept(smPos, sqn, op)

    case Accept_OK(smPos, sqn) =>
      val multipaxos = getPaxosInstance(currentN)
      multipaxos ! Accept_OK(smPos, sqn)

    case ExecuteOperations =>
      var previous = -1
      breakable {
        for ((position, operation) <- operationsToExecute.iterator) {
          if (previous != -1 && previous - position != 1) {
            break
          }
          operation.operationType match {
            case StateMachine.PUT =>
              addClientOperation(operation.asInstanceOf[ClientOperation])
            case StateMachine.ADD_REPLICA =>
              addReplica(operation.asInstanceOf[ReplicaOperation].replica)
            case StateMachine.REMOVE_REPLICA =>
              removeReplica(operation.asInstanceOf[ReplicaOperation].replica)
          }
          operationsToExecute -= position
          previous = position
        }
      }

    case CopyState(reps, smap) =>
      replicas = reps
      serviceMap = smap*/

    case Kill => context.stop(self)

    case PrepareDebug => {
      if (monitorLeaderSchedule != null)
        monitorLeaderSchedule.cancel()
    }
    
    case Debug => log.info("oldId={}. stateMachnine{} -> my leader is {}", oldSqn, mySequenceNumber, currentLeader); context.stop(self)

  }
}
