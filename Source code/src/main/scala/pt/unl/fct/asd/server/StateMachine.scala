package pt.unl.fct.asd
package server

import java.text.SimpleDateFormat
import java.util.Date

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSelection, ActorSystem, Cancellable, Props}
import akka.util.Timeout
import com.typesafe.config.Config

import scala.collection.SortedMap
import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random
import scala.util.control.Breaks._

object StateMachine {
  def props(application: ActorRef, sequenceNumber: Int, replicasInfo: Array[String]): Props =
    Props(new StateMachine(application, sequenceNumber, replicasInfo))
}

class StateMachine(val application: ActorRef, val sequenceNumber: Int, val replicasInfo: Array[String]) extends Actor with ActorLogging {

  var multipaxos: ActorRef = context.actorOf(Multipaxos.props(application, self, sequenceNumber, replicasInfo), "multipaxos"+sequenceNumber)
  var sequencePosition: Int = 0
  var operations: SortedMap[Int, Operation] = SortedMap.empty[Int, Operation]
  var lastExecutedOperationPosition: Int = -1
  var operationsWaiting: ListBuffer[Operation] =  ListBuffer.empty[Operation] // operations waiting to be proposed in the next batch
  var numberOfOperationsProposed: Int = 0
  var leader: ActorRef = _
  var proposeOperationsSchedule: Cancellable = _
  val BATCH_SIZE: Int = 5
  val BATCH_TIME_LIMIT: Int = 500 // forçar um propose se houverem operações à espera durante mais de 500 milliseconds
  var nextBatchTimeLimit: Long = 0


/*  case ExecuteOperations => {
    var opsToExecute = operationsToExecute.drop(positionOfLastOpExecuted)
    if (opsToExecute.nonEmpty) {
      var it = opsToExecute.iterator
      var (previous, operation) = it.next()
      positionOfLastOpExecuted += 1
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
  }*/

  private def executePendingOperations(): Unit = {
    //TODO se a replica estiver ainda a ser copiada, vai executar as operações pela ordem errada
    var previousPosition = -1
    breakable {
      for ((currentPosition, operation) <- operations.drop(lastExecutedOperationPosition + 1).iterator) {
        if (previousPosition != -1 && previousPosition - currentPosition != 1) {
          break
        }
        operation match {
          case WriteOperation(key, value, requestId) =>
            writeValue(key, value, requestId)
          case AddReplicaOperation(replica) =>
            addReplica(replica, currentPosition)
          case RemoveReplicaOperation(replica) =>
            removeReplica(replica, currentPosition)
          case _ =>
            log.error(s"Got unexpected operation $operation")
        }
        lastExecutedOperationPosition = currentPosition
        previousPosition = currentPosition
      }
    }
  }

  private def writeValue(key: String, value: String, requestId: String): Unit = {
    logInfo(s"Executed write: key=$key value=$value")
    application ! WriteResponse(WriteOperation(key, value, requestId))
  }

  private def addReplica(replica: ActorRef, seqPosition: Int): Unit = {
    logInfo(s"Adding replica: $replica at position $seqPosition")
    multipaxos ! AddNewReplica(seqPosition, replica)
    if (leader == self) {
      replica ! SetStateMachineState(sequencePosition, operations.filterKeys(_ < seqPosition))
    }
  }

  private def removeReplica(replica: ActorRef, seqPosition: Int): Unit = {
    logInfo(s"Removing replica: $replica")
    multipaxos ! RemoveOldReplica(seqPosition, replica)
  }

  private def scheduleProposes(): Unit = {
    logInfo("Checking operations batch")
    nextBatchTimeLimit = System.currentTimeMillis() + BATCH_TIME_LIMIT
    proposeOperationsSchedule = context.system.scheduler.schedule(0 millis, BATCH_TIME_LIMIT millis) {
      if (operationsWaiting.size >= BATCH_SIZE || operationsWaiting.nonEmpty && System.currentTimeMillis() >= nextBatchTimeLimit) {
        numberOfOperationsProposed = operationsWaiting.size
        multipaxos ! Propose(sequencePosition, operationsWaiting.toList)
        proposeOperationsSchedule.cancel()
        logInfo(s"Proposed batch $operationsWaiting with size ${operationsWaiting.size}")
      }
    }
  }

  private def logInfo(msg: String): Unit = {
    log.info(s"\n${self.path.name}: $msg")
  }

  override def receive: PartialFunction[Any, Unit] = {

    case msg @ WriteValue(key: String, value: String, requestId: String) =>
      logInfo(s"Got write request key=$key value=$value requestId=$requestId from $sender")
      if (leader != null) {
        if (leader == self) {
          logInfo(s"Got write request $requestId to propose")
          operationsWaiting += WriteOperation(key, value, requestId)
        } else {
          logInfo(s"Write request $requestId redirected to leader $leader")
          leader forward msg
        }
      } else {
        logInfo(s"Write request $requestId ignored. Leader is unknown")
      }

    case msg @ AddReplica(replica: ActorRef) =>
      if (leader != null) {
        if (leader == self) {
          logInfo(s"Got Add replica $replica request to propose")
          operationsWaiting += AddReplicaOperation(replica) //TODO não fazer batch de addreplica
        } else {
          logInfo(s"Add replica $replica request redirected to leader $leader")
          leader forward msg
        }
      } else {
        logInfo(s"Add replica $replica request ignored. Leader is unknown")
      }

    case msg @ RemoveReplica(replica: ActorRef) =>
      if (leader != null) {
        if (leader == self) {
          logInfo(s"Got Remove replica $replica request to propose")
          operationsWaiting += RemoveReplicaOperation(replica)  //TODO não fazer batch de removereplica
        } else {
          logInfo(s"Remove replica $replica request redirected to leader $leader")
          leader forward msg
        }
      } else {
        logInfo(s"Remove replica $replica request ignored. Leader is unknown")
      }

    case Decided(seqPosition: Int, ops: List[Operation]) =>
      logInfo(s"Got decided operations $ops for position $seqPosition")
      ops.zipWithIndex.foreach{case (op, index) => {
        operations += (seqPosition + index) -> op
      }}
      sequencePosition += ops.size
      executePendingOperations()
      if (leader == self) {
        logInfo(s"Total operations waiting $operationsWaiting")
        operationsWaiting = operationsWaiting.drop(numberOfOperationsProposed + 1)
        logInfo(s"Operations waiting after decided $operationsWaiting")
        numberOfOperationsProposed = 0
        scheduleProposes()
      }

    case UpdateLeader(newLeader: Node) =>
      logInfo(s"Updating leader $newLeader")
      leader = newLeader.stateMachine
      if (leader != null) {
        if (leader == self) {
          scheduleProposes()
        } else {
          if (proposeOperationsSchedule != null) {
            proposeOperationsSchedule.cancel()
          }
          operationsWaiting = ListBuffer.empty[Operation]
          numberOfOperationsProposed = 0
          nextBatchTimeLimit = 0
        }
      }
      application ! UpdateLeader(newLeader)

    case SetStateMachineState(seqPosition: Int, ops: SortedMap[Int, Operation]) =>
      logInfo(s"Setting statemachine state with sequencePosition $seqPosition and operations $ops")
      sequencePosition = seqPosition
      operations ++= ops
      leader = sender
      executePendingOperations()

    case Debug =>
      logInfo(s"\n" +
        s"  - Multipaxos: $multipaxos\n" +
        s"  - SequencePosition: $sequencePosition\n" +
        s"  - Operations: $operations\n" +
        s"  - LastExecutedOperationPosition: $lastExecutedOperationPosition\n" +
        s"  - NumberOfOperationsProposed: $numberOfOperationsProposed\n" +
        s"  - OperationsWaiting: $operationsWaiting\n" +
        s"  - Leader: $leader\n" +
        s"  - NextBatchTimeLimit: ${new SimpleDateFormat("dd/MM/yyyy hh:mm:ss.SSS").format(new Date(nextBatchTimeLimit))}")
      multipaxos ! Debug

    case msg @ _ =>
      log.warning(s"\nGot unexpected message $msg from $sender")
  }

}
