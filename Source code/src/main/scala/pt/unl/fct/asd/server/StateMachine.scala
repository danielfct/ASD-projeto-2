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

object StateMachine {
  def props(sequenceNumber: Int, replicasInfo: Array[String], application: ActorRef): Props =
    Props(new StateMachine(sequenceNumber, replicasInfo, application))
}

class StateMachine(sequenceNumber: Int, replicasInfo: Array[String], application: ActorRef) extends Actor with ActorLogging {

  var sequencePosition: Int = 0
  var operationsToExecute: SortedMap[Int, Operation] = SortedMap.empty[Int, Operation]

  var oldSequenceNumber: Int = sequenceNumber
  var currentSequenceNum: Int = sequenceNumber
  var replicas: Set[ActorSelection] = this.selectReplicas(replicasInfo)
  var majority: Int = Math.ceil((replicas.size + 1.0) / 2.0).toInt
  var currentLeader: ActorRef = _
  var oldLeader: ActorRef = _
  var promise: Int = -1
  var prepareAcks: Int = 0

  var monitorLeaderSchedule: Cancellable = _
  var leaderHeartbeatSchedule: Cancellable = _
  var prepareTimeout: Cancellable = _

  var paxos: ActorRef = context.actorOf(Multipaxos.props(self, replicas, sequenceNumber, promise), "multipaxos"+sequenceNumber)

  var leaderLastHeartbeat: Long = 0
  var LEADER_TTL: Long = 6000 // leader sends heartbeats every 3 seconds

  this.overrideLeader()

  def selectReplicas(replicasInfo: Array[String]): Set[ActorSelection] = {
    var replicas: Set[ActorSelection] = Set.empty[ActorSelection]
    replicasInfo.foreach(replica => replicas += context.actorSelection(s"akka.tcp://Server@$replica"))
    this.logInfo(s"Initial replicas: $replicas")
    replicas
  }

  private def overrideLeader(): Unit = {
    this.logInfo(s"Trying to be the leader")
    prepareAcks = 0
    replicas.foreach(replica => {
      this.logInfo(s"Sending prepare to $replica")
      replica ! Prepare(currentSequenceNum)
    })
    if (prepareTimeout != null)
      prepareTimeout.cancel()
    prepareTimeout = context.system.scheduler.scheduleOnce(5 seconds, self, PrepareTimeout)
  }

  private def propose(operation: Operation): Unit = {
    paxos ! Propose(sequencePosition, operation)
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
            log.error(s"stateMachine$sequenceNumber got unexpected operation $operation")
        }
        operationsToExecute -= currentPos
        previousPos = currentPos
      }
    }
  }

  private def write(key: String, value: String, requestId: String): Unit = {
    this.logInfo(s"Executed write: key=$key value=$value")
    application ! WriteResponse(WriteOperation(key, value, requestId))
  }

  private def addReplica(replica: ActorRef): Unit = {
    this.logInfo(s"Added replica: $replica")
    val replicaSelection = context.actorSelection(s"akka.tcp://$replica")
    replicas += replicaSelection
    majority = Math.ceil((replicas.size + 1.0) / 2.0).toInt
    paxos ! AddReplica(replicaSelection)
    /*    if (currentLeader == self)
          replica ! CopyState(replicas, serviceMap)*/
  }

  private def removeReplica(replica: ActorRef): Unit = {
    this.logInfo(s"Removed replica: $replica")
    val replicaSelection = context.actorSelection(s"akka.tcp://$replica")
    replicas -= replicaSelection
    majority = Math.ceil((replicas.size + 1.0) / 2.0).toInt
    paxos ! RemoveReplica(replicaSelection)
  }

  private def logInfo(msg: String): Unit = {
    log.info(s"\n${self.path.name}-$currentSequenceNum: $msg")
  }

  override def receive: PartialFunction[Any, Unit] = {

    case SetLeader(sequenceNumber: Int, leader: Node) =>
      if (sequenceNumber >= promise) {
        this.logInfo(s"Changing leader to $leader because promise $promise < sequenceNumber $sequenceNumber")
        if (leaderHeartbeatSchedule != null) {
          leaderHeartbeatSchedule.cancel()
        }
        if (monitorLeaderSchedule != null) {
          monitorLeaderSchedule.cancel()
        }

        if (leader.stateMachine == self) { // self becomes leader
          leaderHeartbeatSchedule = context.system.scheduler.schedule(3 seconds, 3 seconds, self, SignalLeaderAlive)
          currentSequenceNum = 0
          paxos ! SetSequenceNumber(currentSequenceNum)
          if (oldLeader != null && currentLeader != self) {
            context.system.scheduler.scheduleOnce(5 seconds) {
              if (currentLeader == self) {
                this.logInfo(s"Proposing to remove replica $oldLeader")
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
        currentLeader = leader.stateMachine
        paxos ! UpdateLeader(leader.paxos)
        application ! UpdateLeader(leader.application)
        promise = sequenceNumber
        paxos ! SetPromise(promise)
        if (currentLeader != self) {
          monitorLeaderSchedule = context.system.scheduler.schedule(3 seconds, 3 seconds, self, CheckLeaderAlive)
          leaderLastHeartbeat = System.currentTimeMillis()
        }
      }
      else {
        this.logInfo(s"Refusing to change leader to $leader because promise $promise >= sequenceNumber $sequenceNumber")
      }

    case SignalLeaderAlive =>
      if (currentLeader == self) {
        this.logInfo(s"Sent heartbeat")
        replicas.foreach(replica => replica ! LeaderHeartbeat)
      }
      else {
        leaderHeartbeatSchedule.cancel()
      }

    case LeaderHeartbeat =>
      this.logInfo(s"Got heartbeat from $sender and leader is $currentLeader")
      if (sender == currentLeader) {
        leaderLastHeartbeat = System.currentTimeMillis()
      }

    case CheckLeaderAlive =>
      this.logInfo(s"Checking health of leader $currentLeader")
      if (currentLeader != null) {
        if (System.currentTimeMillis() > leaderLastHeartbeat + LEADER_TTL) {
          monitorLeaderSchedule.cancel()
          oldLeader = currentLeader
          currentLeader = null
          paxos ! UpdateLeader(null)
          application ! UpdateLeader(null)
          this.overrideLeader()
        }
      } else {
        monitorLeaderSchedule.cancel()
      }

    case Prepare(n) =>
      this.logInfo(s"Got prepare from $sender")
      if (n > promise) {
        this.logInfo(s"Sending prepare_ok to $sender because promise $promise < sequenceNumber $n")
        promise = n
        paxos ! SetPromise(promise)
        sender ! Prepare_OK(n, sequencePosition)
      } else {
        this.logInfo(s"Rejecting prepare from $sender because promise $promise >= sequenceNumber $n")
      }
      if (n > currentSequenceNum && prepareTimeout != null) //TODO confirmar isto
        prepareTimeout.cancel()

    case Prepare_OK(n, acceptedN) =>
      this.logInfo(s"Got prepare_ok from $sender")
      if (n == currentSequenceNum) {
        prepareAcks += 1
        if (prepareAcks >= majority) {
          if (prepareTimeout != null)
            prepareTimeout.cancel()
          this.logInfo(s"Got majority. I'm the leader now")
          replicas.foreach(replica => replica ! SetLeader(currentSequenceNum, Node(application, self, paxos)))
        }
      }

    case PrepareTimeout =>
      this.logInfo(s"Prepare timed out. New sequence number = ${currentSequenceNum + replicas.size}")
      currentSequenceNum += replicas.size
      paxos ! SetSequenceNumber(currentSequenceNum)
      oldSequenceNumber = currentSequenceNum
      this.overrideLeader()

    case Decided(stateMachinePosition, operation) =>
      operationsToExecute += stateMachinePosition -> operation
      sequencePosition += 1
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
      this.logInfo(s"\n" +
        s"  - OperationsToExecute: $operationsToExecute\n" +
        s"  - SequencePosition: $sequencePosition\n" +
        s"  - OldSqn: $oldSequenceNumber\n" +
        s"  - currentSequenceNum: $currentSequenceNum\n" +
        s"  - Replicas: $replicas}\n" +
        s"  - Majority: $majority\n" +
        String.format("  - CurrentLeader: %s\n", if (currentLeader == null) "null" else currentLeader.path.name) +
        s"  - Promise: $promise\n" +
        s"  - PrepareAcks: $prepareAcks\n" +
        s"  - Paxos: $paxos\n" +
        s"  - LeaderLastHeartbeat: ${new SimpleDateFormat("dd/MM/yyyy hh:mm:ss.SSS").format(new Date(leaderLastHeartbeat))}\n")

    case _ =>
      this.logInfo(s"Got unexpected message from $sender")
  }

}
