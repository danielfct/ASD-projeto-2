package pt.unl.fct.asd
package client

import java.text.SimpleDateFormat
import java.util.Date

import akka.actor.{Actor, ActorLogging, ActorSelection, Cancellable, Props}
import pt.unl.fct.asd.server._

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.util.Random
import java.io._
import util.control.Breaks._

object Client {
  def props(numberOfOperations: Int, percentageOfWrites: Int, replicasInfo: Array[String]): Props =
    Props(new Client(numberOfOperations, percentageOfWrites, replicasInfo))
}

class Client(var numberOfOperations: Int, val percentageOfWrites: Int, val replicasInfo: Array[String])
  extends Actor with ActorLogging {

  val replicas: Set[ActorSelection] = this.selectReplicas(replicasInfo)
  var requests: Map[String, OperationInfo] = Map.empty[String, OperationInfo] // requestId -> (operation, sendTime, replyTime)
  var lastOperation: Operation = _
  var startTime: Long = 0
  var numberOfReads: Int = 0
  var numberOfWrites: Int = 0
  var numberOfTimeouts: Int = 0
  val OPERATION_TIMEOUT: Int = 10000
  var operationTimeoutSchedule: Cancellable = _
  var writeLatencyThroughputSchedule: Cancellable = _
  val pw = new PrintWriter(new File(s"${self.path.name}_latencyThroughput_" +
    s"$percentageOfWrites%writes_${replicas.size}replicas_${numberOfOperations}ops.txt"))
  val r = new Random

  def selectReplicas(replicasInfo: Array[String]): Set[ActorSelection] = {
    var replicas: Set[ActorSelection] = Set.empty[ActorSelection]
    replicasInfo.foreach(replica => replicas += context.actorSelection(s"akka.tcp://Server@$replica"))
    logInfo(s"\nInitial replicas: $replicas")
    replicas
  }

  private def randomOperation(): Operation = {
    val requestId: String = System.currentTimeMillis().toString
    val randomKey: String = r.nextInt(10).toString
    if (r.nextInt(100) < percentageOfWrites) {
      val randomValue: String = r.nextInt(10).toString
      WriteOperation(randomKey, randomValue, requestId)
    } else {
      ReadOperation(randomKey, requestId)
    }
  }

  private def randomReplica(): ActorSelection = {
    val index = r.nextInt(replicas.size)
    replicas.toSeq(index)
  }

  private def resendRead(key: String): Unit = {
    val replica: ActorSelection = randomReplica()
    logInfo(s"Resent read operation key=$key to $replica")
    replica ! Read(key)
    operationTimeoutSchedule = context.system.scheduler.scheduleOnce(OPERATION_TIMEOUT millis) { operationTimeout() }
  }

  private def resendWrite(key: String, value: String, timestamp: Long): Unit = {
    val replica: ActorSelection = randomReplica()

    logInfo(s"Resent write operation key=$key value=$value to $replica")
    replica ! Write(key, value, timestamp)
    operationTimeoutSchedule = context.system.scheduler.scheduleOnce(OPERATION_TIMEOUT millis) { operationTimeout() }
  }

  private def sendOperation(): Unit = {
    lastOperation = randomOperation()
    val replica: ActorSelection = randomReplica()
    lastOperation match {
      case ReadOperation(key: String, requestId: String) =>
        val timestamp = requestId.toLong
        requests += requestId -> OperationInfo(lastOperation, timestamp, -1L)
        numberOfReads += 1
        replica ! Read(key)
        logInfo(s"Sent read operation key=$key to $replica")
      case WriteOperation(key: String, value: String, requestId: String) =>
        val timestamp = requestId.toLong
        requests += requestId -> OperationInfo(lastOperation, timestamp, -1L)
        numberOfWrites += 1
        replica ! Write(key, value, timestamp)
        logInfo(s"Sent write operation key=$key value=$value to $replica")
      case _ =>
        log.error(s"\nClient can't execute operation $lastOperation")
    }
    operationTimeoutSchedule = context.system.scheduler.scheduleOnce(OPERATION_TIMEOUT millis) { operationTimeout() }
  }

  private def processResponse(): Unit = {
    val responseTimestamp = System.currentTimeMillis()
    lastOperation match {
      case ReadOperation(_, requestId: String) =>
        val sendTimestamp: Long = requests(requestId).sendTimestamp
        requests -= requestId
        requests += requestId -> OperationInfo(lastOperation, sendTimestamp, responseTimestamp)
      case WriteOperation(_, _, requestId: String) =>
        val sendTimestamp: Long = requests(requestId).sendTimestamp
        requests -= requestId
        requests += requestId -> OperationInfo(lastOperation, sendTimestamp, responseTimestamp)
      case _ =>
        log.error(s"\nClient can't recieve response for operation $lastOperation")
    }
  }

  private def operationTimeout(): Unit = {
    logInfo(s"Timeout $lastOperation")
    numberOfTimeouts += 1
    lastOperation match {
      case ReadOperation(key: String, requestId: String) =>
        requests -= requestId
        requests += requestId -> OperationInfo(lastOperation, System.currentTimeMillis(), -1)
        resendRead(key)
      case WriteOperation(key: String, value: String, requestId: String) =>
        requests -= requestId
        requests += requestId -> OperationInfo(lastOperation, System.currentTimeMillis(), -1)
        resendWrite(key, value, requestId.toLong)
      case _ =>
        log.error(s"\nClient can't execute operation $lastOperation")
    }
  }

  private def end(): Unit = {
    val stats: Stats = calculateStats()
    log.info(s"\n" +
      s"Start time: ${new SimpleDateFormat("dd/MM/yyyy hh:mm:ss.SSS").format(new Date(startTime))}\n" +
      s"End time: ${new SimpleDateFormat("dd/MM/yyyy hh:mm:ss.SSS").format(new Date(stats.endTime))}\n" +
      s"Duration: ${stats.durationMillis} milliseconds (${stats.durationSeconds} seconds)\n" +
      s"Average latency: ${stats.averageLatency} milliseconds\n" +
      s"Reads average latency: ${stats.averageReadsLatency} milliseconds\n" +
      s"Writes average latency: ${stats.averageWritesLatency} milliseconds\n" +
      s"Average throughput: ${stats.requestsPerSecond} operations per second\n" +
      s"Number of requests: ${requests.size}\n" +
      s"Number of reads: $numberOfReads\n" +
      s"Number of writes: $numberOfWrites\n" +
      s"Number of timeouts: $numberOfTimeouts\n" +
      s"Percentage of writes: $percentageOfWrites%\n" +
      s"Requests: $requests\n")
    pw.write(s"Start time: ${new SimpleDateFormat("dd/MM/yyyy hh:mm:ss.SSS").format(new Date(startTime))}\n")
    pw.write(s"End time: ${new SimpleDateFormat("dd/MM/yyyy hh:mm:ss.SSS").format(new Date(stats.endTime))}\n")
    pw.write(s"Duration: ${stats.durationMillis} milliseconds (${stats.durationSeconds} seconds)\n")
    pw.write(s"Average latency: ${stats.averageLatency} milliseconds\n")
    pw.write(s"Reads average latency: ${stats.averageReadsLatency} milliseconds\n")
    pw.write(s"Writes average latency: ${stats.averageWritesLatency} milliseconds\n")
    pw.write(s"Average throughput: ${stats.requestsPerSecond} operations per second\n")
    pw.write(s"Number of requests: ${requests.size}\n")
    pw.write(s"Number of reads: $numberOfReads\n")
    pw.write(s"Number of writes: $numberOfWrites\n")
    pw.write(s"Number of timeouts: $numberOfTimeouts\n")
    pw.write(s"Percentage of writes: $percentageOfWrites%\n")
    pw.write(s"Number of replicas: ${replicas.size}\n")
    pw.flush()
    pw.close()
    Thread.sleep(5000)
    replicas.foreach(replica => replica ! Debug)
  }

  private def calculateStats(): Stats = {
    val endTime: Long = System.currentTimeMillis()
    val durationMillis: Long = endTime - startTime
    val durationSeconds: Float = if (durationMillis == 0) 0 else durationMillis/1000f
    val requestsPerSecond: Float = if (durationSeconds == 0) 0 else requests.size/durationSeconds
    var totalLatency: Int = 0
    var totalReadsLatency: Int = 0
    var totalWritesLatency: Int = 0
    for ((_, operationInfo) <- requests) {
      breakable {
        if (operationInfo.responseTimestamp < 0) {
          break // funciona como continue
        }
        val latency: Int = (operationInfo.responseTimestamp - operationInfo.sendTimestamp).toInt
        totalLatency += latency
        operationInfo.operation match {
          case ReadOperation(_, _) =>
            totalReadsLatency += latency
          case WriteOperation(_, _, _) =>
            totalWritesLatency += latency
          case _ =>
            log.error(s"\nClient executed unexpected operation ${operationInfo.operation}")
        }
      }
    }
    val averageLatency: Int = if (requests.isEmpty) 0 else totalLatency/requests.size
    val averageReadsLatency: Int = if (numberOfReads == 0) 0 else totalReadsLatency/numberOfReads
    val averageWritesLatency: Int = if (numberOfWrites == 0) 0 else totalWritesLatency/numberOfWrites
    new Stats(endTime, durationMillis, durationSeconds, requestsPerSecond,
      averageLatency, averageReadsLatency, averageWritesLatency)
  }

  private def writeLatencyThroughput(): Unit = {
    if (numberOfOperations > 0) {
      val stats: Stats = calculateStats()
      pw.write(s"${stats.averageLatency} ${stats.requestsPerSecond}\n")
    } else {
      writeLatencyThroughputSchedule.cancel()
    }
  }

  private def logInfo(msg: String): Unit = {
   /* log.info(s"\n${self.path.name}: $msg")*/
  }

  override def receive: Receive = {
    case "START" =>
      startTime = System.currentTimeMillis()
      if (numberOfOperations > 0) {
        writeLatencyThroughputSchedule = context.system.scheduler.schedule(250 millis, 250 millis) { writeLatencyThroughput() }
        sendOperation()
      } else {
        end()
      }

    case Response(result: Option[String]) =>
      logInfo(s"Got response value=$result \nOperations left: ${numberOfOperations-1}")
      operationTimeoutSchedule.cancel()
      processResponse()
      numberOfOperations -= 1
      if (numberOfOperations > 0) {
        sendOperation()
      } else {
        end()
      }

  }

}
