package pt.unl.fct.asd
package server
import java.text.SimpleDateFormat
import java.util.Date

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSelection, Cancellable, Props}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

object Multipaxos {
  def props(application: ActorRef, stateMachine: ActorRef, sequenceNumber: Int, replicasInfo: Array[String]): Props = Props(
    new Multipaxos(application, stateMachine, sequenceNumber, replicasInfo))
}

class Multipaxos(val application: ActorRef, val stateMachine: ActorRef, var sequenceNumber: Int, val replicasInfo: Array[String])
  extends Actor with ActorLogging {

  var replicas: Set[ActorSelection] = selectReplicas(replicasInfo)
  var leader: ActorRef = _
  var oldLeader: ActorRef = _
  var majority: Int = Math.ceil((replicas.size + 1.0) / 2.0).toInt
  var sequencePosition: Int = 0
  var operations: List[Operation] = List.empty[Operation]
  var acceptedAcks: Int = 0
  var acceptTimeoutSchedule: Cancellable = _
  var promise: Int = 0
  var prepareAcks: Int = 0
  var prepareTimeoutSchedule: Cancellable = _
  var monitorLeaderSchedule: Cancellable = _
  var leaderHeartbeatSchedule: Cancellable = _
  var leaderLastHeartbeat: Long = 0
  val LEADER_TTL: Long = 6000 // leader sends heartbeats every 3 seconds

  start()

  def selectReplicas(replicasInfo: Array[String]): Set[ActorSelection] = {
    var replicas: Set[ActorSelection] = Set.empty[ActorSelection]
    replicasInfo.foreach(replica => replicas += context.actorSelection(s"akka.tcp://Server@$replica"))
    logInfo(s"Initial replicas: $replicas")
    replicas
  }

  private def start(): Unit = {
    if (replicas.size == 1) { // Assumimos que o set inicial de replicas é maior que 1
      val contactNode: ActorSelection = replicas.toSeq.head
      contactNode ! AddReplica(self)
    } else {
      overrideLeader()
    }
  }

  private def overrideLeader(): Unit = {

    logInfo(s"Trying to be the leader")
    leader = null
    stateMachine ! UpdateLeader(Node(null, null, null))
    prepareAcks = 0
    replicas.foreach(replica => {
      logInfo(s"Sending prepare to $replica")
      replica ! Prepare(sequencePosition, sequenceNumber)
    })
    if (prepareTimeoutSchedule != null) {
      prepareTimeoutSchedule.cancel()
    }
    prepareTimeoutSchedule = context.system.scheduler.scheduleOnce(5 seconds) { prepareTimeout() }
  }

  private def signalLeaderAlive(): Unit = {
    if (leader == self) {
      logInfo(s"Sent heartbeat")
      replicas.foreach(replica => replica ! LeaderHeartbeat)
    }
    else {
      leaderHeartbeatSchedule.cancel()
    }
  }

  private def checkLeaderAlive(): Unit = {
    logInfo(s"Checking health of leader $leader")
    if (leader != null) {
      if (System.currentTimeMillis() > leaderLastHeartbeat + LEADER_TTL) {
        monitorLeaderSchedule.cancel()
        oldLeader = leader
        overrideLeader()
      }
    } else {
      monitorLeaderSchedule.cancel()
    }
  }

  private def prepareTimeout(): Unit = {
    logInfo(s"Prepare timed out. New sequence number = ${sequenceNumber + replicas.size}")
    sequenceNumber += replicas.size
    overrideLeader()
  }

  private def proposeOldLeaderRemoval(): Unit = {
    if (leader == self) {
      logInfo(s"Proposing to remove replica $oldLeader")
      stateMachine ! RemoveReplica(oldLeader)
      oldLeader = null
    }
  }

  private def logInfo(msg: String): Unit = {
    /*log.info(s"\n${self.path.name}-$sequenceNumber: $msg")*/
  }

  override def receive: PartialFunction[Any, Unit] = {

    case msg @ AddReplica(replica: ActorRef) =>
      logInfo(s"Got add replica request $replica")
      stateMachine ! msg

    case SetLeader(seqNumber: Int, newLeader: Node) =>
      if (seqNumber >= promise) {
        logInfo(s"Changing leader to $newLeader because promise $promise <= sequenceNumber $seqNumber")
        if (leaderHeartbeatSchedule != null) {
          leaderHeartbeatSchedule.cancel()
        }
        if (monitorLeaderSchedule != null) {
          monitorLeaderSchedule.cancel()
        }
        if (newLeader.multipaxos == self) { // self becomes leader
          leaderHeartbeatSchedule = context.system.scheduler.schedule(3 seconds, 3 seconds) { signalLeaderAlive() }
          if (oldLeader != null && leader != self) {
            context.system.scheduler.scheduleOnce(5 seconds) { proposeOldLeaderRemoval() }
          }
        }
        leader = newLeader.multipaxos
        stateMachine ! UpdateLeader(newLeader)
        if (leader != self) {
          monitorLeaderSchedule = context.system.scheduler.schedule(3 seconds, 3 seconds) { checkLeaderAlive() }
          leaderLastHeartbeat = System.currentTimeMillis()
        }
      }
      else {
        logInfo(s"Refusing to change leader to $leader because promise $promise >= sequenceNumber $seqNumber")
      }

    case LeaderHeartbeat =>
      logInfo(s"Got heartbeat from $sender and leader is $leader")
      if (sender == leader) {
        leaderLastHeartbeat = System.currentTimeMillis()
      }

    case Prepare(seqPosition: Int, seqNumber: Int) =>
      logInfo(s"Got prepare from $sender")
      if (seqNumber > promise) {
        logInfo(s"Sending prepare_ok to $sender because promise $promise < sequenceNumber $seqNumber")
        promise = seqNumber
        sender ! Prepare_OK(seqPosition, seqNumber)
      } else {
        logInfo(s"Rejecting prepare from $sender because promise $promise >= sequenceNumber $seqNumber")
      }
      if (seqNumber > sequenceNumber && prepareTimeoutSchedule != null)
        prepareTimeoutSchedule.cancel()

    case Prepare_OK(seqPosition: Int, seqNumber: Int) =>
      logInfo(s"Got prepare_ok from $sender")
      if (seqNumber == sequenceNumber) {
        prepareAcks += 1
        if (prepareAcks == majority) {
          if (prepareTimeoutSchedule != null)
            prepareTimeoutSchedule.cancel()
          logInfo(s"Got majority. I'm the leader now")
          replicas.foreach(replica => replica ! SetLeader(sequenceNumber, Node(application, stateMachine, self)))
        }
      }

    case Propose(seqPosition: Int, ops: List[Operation]) =>
      if (leader == self) {
        logInfo(s"Proposing $ops for position $seqPosition")
        sequencePosition = seqPosition
        operations = ops
        acceptedAcks = 0
        replicas.foreach(replica => replica ! Accept(seqPosition, sequenceNumber, ops))
        acceptTimeoutSchedule = context.system.scheduler.scheduleOnce(5 seconds) {
          logInfo(s"Accept timed out! New sequence number = ${sequenceNumber + replicas.size}")
          sequenceNumber = sequenceNumber + replicas.size
          self ! Propose(sequencePosition, operations)
        }
      } else {
        logInfo(s"Rejecting to propose $ops for position $seqPosition. Leader is $leader")
      }

    case Accept(seqPosition: Int, seqNumber: Int, ops: List[Operation]) =>
      logInfo(s"Got accept request from $sender with sequenceNumber $seqNumber to position $seqPosition")
      if (seqNumber >= promise && sender == leader) {
        logInfo(s"Accepted operation $ops for position $seqPosition")
        promise = 0
        sequencePosition = seqPosition
        operations = ops
        sender ! Accept_OK(seqPosition, seqNumber, ops)
      } else {
        logInfo(s"Rejected operations $ops from $sender. Promise is $promise, SequenceNumber is $seqNumber and leader is $leader")
      }

    case Accept_OK(seqPosition: Int, seqNumber: Int, ops: List[Operation]) =>
      logInfo(s"Got accept_ok from $sender with sequenceNumber $seqNumber, position $seqPosition and operations $ops")
      if (seqNumber == sequenceNumber) {
        logInfo(s"Accepted accept_ok for position $seqPosition with sequenceNumber $seqNumber and operations $ops")
        acceptedAcks += 1
        if (acceptedAcks == majority) {
          logInfo(s"Got accept_ok from majority for position $seqPosition")
          sequenceNumber = 0
          if (acceptTimeoutSchedule != null) {
            acceptTimeoutSchedule.cancel()
          }
          replicas.foreach(replica => replica ! Decided(seqPosition, ops))
        }
      } else {
        logInfo(s"Rejected accept_ok with sequenceNumber $seqNumber because sequenceNumber $seqNumber != mySequenceNumber $sequenceNumber")
      }

    case msg @ Decided(_, _) =>
      stateMachine ! msg

    case AddNewReplica(seqPosition: Int, replica: ActorRef) =>
      logInfo(s"Adding replica $replica")
      val replicaSelection = context.actorSelection(s"akka.tcp://$replica")
      replicas += replicaSelection
      majority = Math.ceil((replicas.size + 1.0) / 2.0).toInt
      if (leader == self) {
        replica ! SetMultiPaxosState(seqPosition, replicas, promise)
      }

    case RemoveOldReplica(seqPosition: Int, replica: ActorRef) =>
      logInfo(s"Removing replica $replica")
      val replicaSelection = context.actorSelection(s"akka.tcp://$replica")
      replicas -= replicaSelection
      majority = Math.ceil((replicas.size + 1.0) / 2.0).toInt

    case SetMultiPaxosState(seqPosition: Int, newReplicas: Set[ActorSelection], newPromise: Int) =>
      logInfo(s"Setting multipaxos state with sequencePosition $seqPosition, replicas $newReplicas and promise $promise")
      replicas = newReplicas
      majority = Math.ceil((replicas.size + 1.0) / 2.0).toInt
      leader = sender
      promise = newPromise
      sequencePosition = seqPosition
      monitorLeaderSchedule = context.system.scheduler.schedule(3 seconds, 3 seconds) { checkLeaderAlive() }
      leaderLastHeartbeat = System.currentTimeMillis()

    case msg @ SetStateMachineState(_, _) =>
      stateMachine ! msg

    case Debug =>
      log.info(s"\n" +
        s"  - Replicas: $replicas\n" +
        s"  - Leader: $leader\n" +
        s"  - OldLeader: $oldLeader\n" +
        s"  - Majority: $majority\n" +
        s"  - SequencePosition: $sequencePosition\n" +
        s"  - Operations: $operations\n" +
        s"  - AcceptedAcks: $acceptedAcks\n" +
        s"  - Promise: $promise\n" +
        s"  - PrepareAcks: $prepareAcks\n" +
        s"  - LeaderLastHeartbeat: ${new SimpleDateFormat("dd/MM/yyyy hh:mm:ss.SSS").format(new Date(leaderLastHeartbeat))}")

    case msg @ _ =>
      log.error(s"\nGot unexpected message $msg from $sender")

  }
}
