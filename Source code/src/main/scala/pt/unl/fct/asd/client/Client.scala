package pt.unl.fct.asd
package client

import java.text.SimpleDateFormat
import java.util.Date

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSelection, ActorSystem, Cancellable, Props}
import com.typesafe.config.Config
import pt.unl.fct.asd.server._

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.util.Random

object Client extends App {
  if (args.length < 6) {
    println("Usage: \"sbt runMain Client initialDelay numberOfClients numberOfOperations ip port replica1 [replica2, ...]\"")
    System.exit(1)
  }
  val initialDelay: Long = args(0).toLong
  val numberOfClients: Int = args(1).toInt
  val numberOfOperations: Int = args(2).toInt
  val hostname: String = args(3)
  val port: Int = args(4).toInt
  val replicasInfo: Array[String] = args.drop(5)
  val config: Config = buildConfiguration(hostname, port)

  implicit val system: ActorSystem = ActorSystem("Client", config)
  for (i <- 1 to numberOfClients) {
    val client = system.actorOf(Client.props(initialDelay, numberOfOperations, replicasInfo), s"client$i")
    client ! "START"
  }

  def props(initialDelay: Long, numberOfOperations: Int, replicasInfo: Array[String]): Props =
    Props(new Client(initialDelay, numberOfOperations, replicasInfo))
}

class Client(val initialDelay: Long, var numberOfOperations: Int, val replicasInfo: Array[String]) extends Actor with ActorLogging {

  val replicas: Set[ActorSelection] = this.selectReplicas(replicasInfo)
  var requests: Map[String, OperationInfo] = Map.empty[String, OperationInfo] // requestId -> (operation, sendTime, replyTime)
  var lastOperation: Operation = _
  var startTime: Long = 0
  var numberOfReads: Int = 0
  var numberOfWrites: Int = 0
  var numberOfTimeouts: Int = 0
  val OPERATION_TIMEOUT: Int = 10000
  var operationTimeoutSchedule: Cancellable = _
  val r = new Random

  def selectReplicas(replicasInfo: Array[String]): Set[ActorSelection] = {
    var replicas: Set[ActorSelection] = Set.empty[ActorSelection]
    replicasInfo.foreach(replica => replicas += context.actorSelection(s"akka.tcp://Server@$replica"))
    this.logInfo(s"\nInitial replicas: $replicas")
    replicas
  }

  private def randomOperation(): Operation = {
    val requestId: String = System.currentTimeMillis().toString
    val randomKey: String = r.nextInt(10).toString
    if (r.nextInt(2) == 0) {
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
    this.logInfo(s"Resent read operation key=$key")
    randomReplica ! Read(key)
    operationTimeoutSchedule = context.system.scheduler.scheduleOnce(OPERATION_TIMEOUT millis) { operationTimeout() }
  }

  private def resendWrite(key: String, value: String, timestamp: Long): Unit = {
    this.logInfo(s"Resent write operation key=$key value=$value")
    randomReplica ! Write(key, value, timestamp)
    operationTimeoutSchedule = context.system.scheduler.scheduleOnce(OPERATION_TIMEOUT millis) { operationTimeout() }
  }

  private def sendOperation(): Unit = {
    lastOperation = randomOperation()
    lastOperation match {
      case ReadOperation(key: String, requestId: String) =>
        val timestamp = requestId.toLong
        requests += requestId -> OperationInfo(lastOperation, timestamp, -1L)
        numberOfReads += 1
        randomReplica ! Read(key)
        this.logInfo(s"Sent read operation key=$key")
      case WriteOperation(key: String, value: String, requestId: String) =>
        val timestamp = requestId.toLong
        requests += requestId -> OperationInfo(lastOperation, timestamp, -1L)
        numberOfWrites += 1
        randomReplica ! Write(key, value, timestamp)
        this.logInfo(s"Sent write operation key=$key value=$value")
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
    this.logInfo(s"Timeout $lastOperation")
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
    val endTime: Long = System.currentTimeMillis()
    val durationMillis: Long = endTime - startTime
    val durationSeconds: Float = if (durationMillis == 0) 0 else durationMillis/1000f
    var totalReadsLatency: Int = 0
    var totalWritesLatency: Int = 0
    var totalLatency: Int = 0
    for ((_, operationInfo) <- requests) {
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
    val requestsPerSecond: Float = if (durationMillis == 0) 0 else requests.size/durationSeconds
    this.logInfo(s"\n" +
      s"Start time: ${new SimpleDateFormat("dd/MM/yyyy hh:mm:ss.SSS").format(new Date(startTime))}\n" +
      s"End time: ${new SimpleDateFormat("dd/MM/yyyy hh:mm:ss.SSS").format(new Date(endTime))}\n" +
      s"Duration: $durationMillis milliseconds ($durationSeconds seconds)\n" +
      s"Reads average latency: ${totalReadsLatency/numberOfReads} milliseconds\n" +
      s"Writes average latency: ${totalWritesLatency/numberOfWrites} milliseconds\n" +
      s"Average latency: ${totalLatency/requests.size} milliseconds\n" +
      s"Speed of requests: $requestsPerSecond per second\n" +
      s"Number of requests: ${requests.size}\n" +
      s"Number of reads: $numberOfReads\n" +
      s"Number of writes: $numberOfWrites\n" +
      s"Number of timeouts: $numberOfTimeouts\n" +
      s"Requests: $requests\n")
    Thread.sleep(10000)
    replicas.foreach(replica => replica ! Debug)
  }

  private def logInfo(msg: String): Unit = {
    log.info(s"\n${self.path.name}: $msg")
  }

  override def receive: Receive = {
    /* Experiments should report the latency and throughput of the designed system
      (ideally through a latency throughout graph) for different number of concurrent clients.
      Additionally, students might also try to evaluate the effects on
      latency and throughput of replicas failing or joining the system).*/

    case "START" =>
      this.logInfo(s"Sleeping for $initialDelay milliseconds")
      Thread.sleep(initialDelay)
      startTime = System.currentTimeMillis()
      if (numberOfOperations > 0) {
        this.sendOperation()
      } else {
        this.end()
      }

    case Response(result: Option[String]) =>
      this.logInfo(s"Got response value=$result \nOperations left: ${numberOfOperations-1}")
      operationTimeoutSchedule.cancel()
      this.processResponse()
      numberOfOperations -= 1
      if (numberOfOperations > 0) {
        this.sendOperation()
      } else {
        this.end()
      }

  }
}
