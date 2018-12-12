package pt.unl.fct.asd
package server

import java.text.SimpleDateFormat
import java.util.Date

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSelection, ActorSystem, Cancellable, Props}
import akka.util.Timeout
import com.typesafe.config.Config

import scala.collection.SortedMap
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random
import scala.util.control.Breaks._

/*object StateMachine extends App {
  if (args.length < 6) {
    println("Usage: \"sbt runMain StateMachine sequenceNumber ip port replica1 replica2 replica3 [replica4, ...]\"")
    System.exit(1)
  }

  val sequenceNumber = args(0).toInt
  val hostname = args(1)
  val port = args(2)
  val replicasInfo = args.drop(3)
  val config: Config = buildConfiguration(hostname, port)

  implicit val system: ActorSystem = ActorSystem("StateMachineSystem", config)
  system.actorOf(StateMachine.props(sequenceNumber, replicasInfo), s"stateMachine$sequenceNumber")

 def props(sequenceNumber: Int, replicasInfo: Array[String]): Props =
Props(new StateMachine(sequenceNumber, replicasInfo))
}*/

object StateMachine {
  def props(sequenceNumber: Int, replicasInfo: Array[String]): Props =
    Props(new StateMachine(sequenceNumber, replicasInfo))
}

class StateMachine(sequenceNumber: Int, replicasInfo: Array[String]) extends Actor with ActorLogging {
  override def preStart(): Unit = log.info(s"\nstateMachine$sequenceNumber has started!")
  override def postStop(): Unit = log.info(s"\nstateMachine$sequenceNumber has stopped!")

  var currentN: Int = 0
  var operationsToExecute: SortedMap[Int, Operation] = SortedMap.empty[Int, Operation]

  var oldSequenceNumber: Int = sequenceNumber
  var currentSequenceNum: Int = sequenceNumber
  var replicas: Set[ActorSelection] = this.selectReplicas(replicasInfo)
  var majority: Int = Math.ceil((replicas.size + 1.0) / 2.0).toInt
  var currentLeader: ActorRef = _
  var oldLeader: ActorRef = _
  var myPromise: Int = -1
  var prepareAcks: Int = 0

  var monitorLeaderSchedule: Cancellable = _
  var leaderHeartbeatSchedule: Cancellable = _
  var prepareTimeout: Cancellable = _

  var paxos: ActorRef = context.actorOf(Multipaxos.props(self, replicas, currentSequenceNum, myPromise), "multipaxos"+currentSequenceNum)

  var leaderLastHeartbeat: Long = 0
  var TTL: Long = 6000 // leader sends heartbeats every 3 seconds

  this.overrideLeader()

  def selectReplicas(replicasInfo: Array[String]): Set[ActorSelection] = {
    Thread.sleep(15000)
    var replicas: Set[ActorSelection] = Set.empty[ActorSelection]
    for (i <- replicasInfo.indices) {
      val replicaInfo: String = replicasInfo(i)
      replicas += context.actorSelection(s"akka.tcp://Server@$replicaInfo")
    }
    log.info(s"\nInitial replicas: $replicas")
    replicas
  }

  private def write(key: String, value: String, requestId: String): Unit = {
    log.info(s"\nExecuted write: key=$key value=$value")
    val appInfo: Array[String] = requestId.split(":")
    val app = context.actorSelection(s"akka.tcp://${appInfo(0)}")
    app ! WriteResponse(WriteOperation(key, value, requestId))
  }

  private def addReplica(replica: ActorRef): Unit = {
    log.info(s"\nAdded replica: $replica")
    val replicaSelection = context.actorSelection(s"akka.tcp://$replica")
    replicas += replicaSelection
    majority = Math.ceil((replicas.size + 1.0) / 2.0).toInt
    paxos ! AddReplica(replicaSelection)
/*    if (currentLeader == self)
      replica ! CopyState(replicas, serviceMap)*/
  }

  private def removeReplica(replica: ActorRef): Unit = {
    log.info(s"\nRemoved replica: $replica")
    val replicaSelection = context.actorSelection(s"akka.tcp://$replica")
    replicas -= replicaSelection
    majority = Math.ceil((replicas.size + 1.0) / 2.0).toInt
    paxos ! RemoveReplica(replicaSelection)
  }

  private def propose(operation: Operation): Unit = {
    paxos ! Propose(currentN, operation)
  }

  private def overrideLeader(): Unit = {
    prepareAcks = 0
    replicas.foreach(rep => rep ! Prepare(currentSequenceNum))
    if (prepareTimeout != null)
      prepareTimeout.cancel()
    prepareTimeout = context.system.scheduler.scheduleOnce(5 seconds, self, PrepareTimeout)
    log.info(s"\n${self.path.name} tries to be the leader")
  }

  private def executeOperations(): Unit = {
    var previousPos = -1
    breakable {
      for ((currentPos, operation) <- operationsToExecute.iterator) {
        if (previousPos != -1 && previousPos - currentPos != 1) {
          break
        }
        operation match {
          case WriteOperation(key, value, requestId) =>
            write(key, value, requestId)
          case AddReplicaOperation(replica) =>
            addReplica(replica)
          case RemoveReplicaOperation(replica) =>
            removeReplica(replica)
          case _ =>
            log.error(s"stateMachine$sequenceNumber got unwanted operation $operation")
        }
        operationsToExecute -= currentPos
        previousPos = currentPos
      }
    }
  }

  override def receive: PartialFunction[Any, Unit] = {

    case SetLeader(sequenceNumber, leader) =>
      Thread.sleep(Random.nextInt(1000))
      if (sequenceNumber >= myPromise) {
        log.info(s"\n${self.path.name}.$currentSequenceNum changes its leader to ${leader.path.name}.$sequenceNumber ($sequenceNumber >= $myPromise)")
        if (leaderHeartbeatSchedule != null) {
          leaderHeartbeatSchedule.cancel()
        }
        if (monitorLeaderSchedule != null) {
          monitorLeaderSchedule.cancel()
        }

        if (leader == self) { // self becomes leader
          leaderHeartbeatSchedule = context.system.scheduler.schedule(3 seconds, 3 seconds, self, SignalLeaderAlive)
          currentSequenceNum = 0
          paxos ! SetSequenceNumber(currentSequenceNum)
          if (oldLeader != null && currentLeader != self) {
            context.system.scheduler.scheduleOnce(5 seconds) {
              if (currentLeader == self) {
                log.info("I WILL PROPOSE REMOTION!")
                self ! SMPropose(RemoveReplicaOperation(oldLeader))
                oldLeader = null
              }
            }
          }
        }
        else if (currentLeader == self) { // self is no longer leader
          currentSequenceNum = oldSequenceNumber
          paxos ! SetSequenceNumber(currentSequenceNum)
        }
        currentLeader = leader
        paxos ! SetLeader(-1,leader)
        myPromise = sequenceNumber
        paxos ! SetPromise(myPromise)
        if (currentLeader != self) {
          monitorLeaderSchedule = context.system.scheduler.schedule(3 seconds, 3 seconds, self, CheckLeaderAlive)
          leaderLastHeartbeat = System.currentTimeMillis()
        }
      }
      else {
        log.info(s"\n${self.path.name}.$currentSequenceNum refuses to change its leader to ${leader.path.name}.$sequenceNumber ($sequenceNumber < $myPromise)")
      }

    case SignalLeaderAlive =>
      if (currentLeader == self) {
        log.info(s"\n${self.path.name} sent heartbeat")
        replicas.foreach(rep => if (rep != self) rep ! LeaderHeartbeat)
      }
      else {
        leaderHeartbeatSchedule.cancel()
      }

    case LeaderHeartbeat =>
      log.info(s"\n${self.path.name} got heartbeat from ${sender.path.name}")
      if (currentLeader != null && !sender.path.name.equals(currentLeader.path.name)) {
        log.error(s"Got heartbeat from ${sender.path.name} and leader is ${currentLeader.path.name}")
      }
      if (sender == currentLeader) {
        leaderLastHeartbeat = System.currentTimeMillis()
      }

    case CheckLeaderAlive =>
      if (currentLeader != null) {
        log.info(s"\n${self.path.name} checking health of leader ${currentLeader.path.name}")
        if (System.currentTimeMillis() > leaderLastHeartbeat + TTL) {
          monitorLeaderSchedule.cancel()
          oldLeader = currentLeader
          currentLeader = null
          paxos ! SetLeader(-1, null)
          this.overrideLeader()
        }
      } else {
        monitorLeaderSchedule.cancel()
      }

    case Prepare(n) =>
      if (n > myPromise) {
        log.info(s"\n${self.path.name}.$currentSequenceNum sends prepare_ok to ${sender.path.name}.$n ($n > $myPromise)")
        myPromise = n
        paxos ! SetPromise(myPromise)
        sender ! Prepare_OK(n, currentN)
      } else {
        log.info(s"\n${self.path.name}.$currentSequenceNum rejects prepare from ${sender.path.name}.$n ($n <= $myPromise)")
      }
      if (n > currentSequenceNum && prepareTimeout != null)
        prepareTimeout.cancel()

    case Prepare_OK(n, acceptedN) =>
      Thread.sleep(Random.nextInt(1000))
      if (n == currentSequenceNum) {
        prepareAcks += 1
        log.info(s"\n${self.path.name}.$currentSequenceNum got prepare_ok from ${sender.path.name}")
        if (prepareAcks >= majority) {
          if (prepareTimeout != null)
            prepareTimeout.cancel()
          log.info(s"\n${self.path.name}.$currentSequenceNum got majority. I'm the leader now")
          replicas.foreach(rep => rep ! SetLeader(currentSequenceNum, self))
        }
      }

    case PrepareTimeout =>
      log.info(s"\n${self.path.name}.$currentSequenceNum timed out! New sqn=${currentSequenceNum + replicas.size}")
      currentSequenceNum = currentSequenceNum + replicas.size
      paxos ! SetSequenceNumber(currentSequenceNum)
      oldSequenceNumber = currentSequenceNum
      prepareAcks = 0
      this.overrideLeader()

    case Decided(stateMachinePosition, operation) =>
      operationsToExecute += stateMachinePosition -> operation
      currentN += 1
      this.executeOperations()

    case msg @ Accept(stateMachinePosition, sequenceNumber, operation) =>
      paxos forward msg

    case msg @ Accept_OK(sequenceNumber, stateMachinePosition) =>
      paxos forward msg


/*    case SetState(sequenceNumber: Int, stateMachinePosition: Int, operation: Operation) =>
      sqnOfAcceptedOp = sequenceNumber
      acceptedN = stateMachinePosition
      acceptedOp = operation*/

    case SetSequenceNumber(sequenceNumber: Int) =>
      currentSequenceNum = sequenceNumber

    case WriteValue(key: String, value: String, requestId: String) =>
      this.propose(WriteOperation(key, value, requestId))

    case Debug =>
      log.info(s"\nDebug for ${self.path.name}:\n" +
        s"  - OperationsToExecute: $operationsToExecute\n" +
        s"  - CurrentN: $currentN\n" +
        s"  - OldSqn: $oldSequenceNumber\n" +
        s"  - currentSequenceNum: $currentSequenceNum\n" +
        s"  - Replicas: $replicas}\n" +
        s"  - Majority: $majority\n" +
        String.format("  - CurrentLeader: %s\n", if (currentLeader == null) "null" else currentLeader.path.name) +
        s"  - MyPromise: $myPromise\n" +
        s"  - PrepareAcks: $prepareAcks\n" +
        s"  - Paxos: $paxos\n" +
        s"  - LeaderLastHeartbeat: ${new SimpleDateFormat("dd/MM/yyyy hh:mm:ss.SSS").format(new Date(leaderLastHeartbeat))}\n")
  }

}
