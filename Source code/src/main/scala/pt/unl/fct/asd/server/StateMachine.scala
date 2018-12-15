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
    log.info(s"\nInitial replicas: $replicas")
    replicas
  }

  private def overrideLeader(): Unit = {
    log.info(s"\nTrying to be the leader")
    prepareAcks = 0
    replicas.foreach(replica => {
      log.info(s"\nSending prepare to $replica")
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
    log.info(s"\nExecuted write: key=$key value=$value")
    application ! WriteResponse(WriteOperation(key, value, requestId))
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

  override def receive: PartialFunction[Any, Unit] = {

    case SetLeader(sequenceNumber: Int, leader: Node) =>
      if (sequenceNumber >= promise) {
        log.info(s"\n${self.path.name}.$currentSequenceNum changes its leader to $leader ($sequenceNumber >= $promise)")
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
        log.info(s"\n${self.path.name}.$currentSequenceNum refuses to change its leader to ${leader.stateMachine.path.name}.$sequenceNumber ($sequenceNumber < $promise)")
      }

    case SignalLeaderAlive =>
      if (currentLeader == self) {
        log.info(s"\n${self.path.name} sent heartbeat")
        replicas.foreach(rep => rep ! LeaderHeartbeat)
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
      log.info(s"\nGot prepare from ${sender.path.name}")
      if (n > promise) {
        log.info(s"\nSending prepare_ok to ${sender.path.name} ($n > $promise)")
        promise = n
        paxos ! SetPromise(promise)
        sender ! Prepare_OK(n, sequencePosition)
      } else {
        log.info(s"\nRejecting prepare from ${sender.path.name} ($n <= $promise)")
      }
      if (n > currentSequenceNum && prepareTimeout != null) //TODO confirmar isto
        prepareTimeout.cancel()

    case Prepare_OK(n, acceptedN) =>
      log.info(s"\nGot prepare_ok from ${sender.path.name}")
      if (n == currentSequenceNum) {
        prepareAcks += 1
        if (prepareAcks >= majority) {
          if (prepareTimeout != null)
            prepareTimeout.cancel()
          log.info(s"\nGot majority. I'm the leader now")
          replicas.foreach(replica => replica ! SetLeader(currentSequenceNum, Node(application, self, paxos)))
        }
      }

    case PrepareTimeout =>
      log.info(s"\nPrepare timed out! New sequence number = ${currentSequenceNum + replicas.size}")
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
      log.info(s"\nDebug for ${self.path.name}:\n" +
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
      log.info(s"Got unexpected message from $sender")
  }

}
