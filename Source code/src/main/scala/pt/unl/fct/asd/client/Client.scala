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
  var requests: Map[OperationInfo, Operation] = Map.empty[OperationInfo, Operation]
  var lastOperation: Operation = _
  var startTime: Long = -1L
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
    this.logInfo(s"\nResent read operation key=$key")
    randomReplica ! Read(key)
    operationTimeoutSchedule = context.system.scheduler.scheduleOnce(OPERATION_TIMEOUT millis, self, OperationTimeout(lastOperation))
  }

  private def resendWrite(key: String, value: String, timestamp: Long): Unit = {
    this.logInfo(s"\nResent write operation key=$key value=$value")
    randomReplica ! Write(key, value, timestamp)
    operationTimeoutSchedule = context.system.scheduler.scheduleOnce(OPERATION_TIMEOUT millis, self, OperationTimeout(lastOperation))
  }

  private def sendOperation(): Unit = {
    lastOperation = randomOperation()
    lastOperation match {
      case ReadOperation(key: String, requestId: String) =>
        val timestamp = requestId.toLong
        requests += OperationInfo(timestamp, -1L) -> lastOperation
        numberOfReads += 1
        randomReplica ! Read(key)
        this.logInfo(s"Sent read operation key=$key")
      case WriteOperation(key: String, value: String, requestId: String) =>
        val timestamp = requestId.toLong
        requests += OperationInfo(timestamp, -1L) -> lastOperation
        numberOfWrites += 1
        randomReplica ! Write(key, value, timestamp)
        this.logInfo(s"Sent write operation key=$key value=$value")
      case _ =>
        log.warning(s"\nClient can't execute operation $lastOperation")
    }
    operationTimeoutSchedule = context.system.scheduler.scheduleOnce(OPERATION_TIMEOUT millis, self, OperationTimeout(lastOperation))
  }

  private def processResponse(): Unit = {
    lastOperation match {
      case ReadOperation(_, requestId: String) =>
        requests = requests.filterKeys(k => !k.equals(OperationInfo(requestId.toLong, -1)))
        requests += OperationInfo(requestId.toLong, System.currentTimeMillis()) -> lastOperation
      case WriteOperation(_, _, requestId: String) =>
        requests = requests.filterKeys(k => !k.equals(OperationInfo(requestId.toLong, -1)))
        requests += OperationInfo(requestId.toLong, System.currentTimeMillis()) -> lastOperation
      case _ =>
        log.warning(s"\nClient can't recieve response for operation $lastOperation")
    }
  }

  private def end(): Unit = {
    printStats()
  }

  private def printStats(): Unit = {
    val endTime: Long = System.currentTimeMillis()
    val durationMillis: Long = endTime - startTime
    val durationSeconds: Float = if (durationMillis == 0) 0 else durationMillis/1000f
    var totalReadsLatency: Int = 0
    var totalWritesLatency: Int = 0
    var totalLatency: Int = 0
    for ((operationInfo, operation) <- requests) {
      val latency: Int = (operationInfo.responseTimestamp - operationInfo.sendTimestamp).toInt
      totalLatency += latency
      operation match {
        case ReadOperation(_, _) =>
          totalReadsLatency += latency
        case WriteOperation(_, _, _) =>
          totalWritesLatency += latency
        case _ =>
          log.warning(s"\nClient executed unexpected operation $operation")
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

    case OperationTimeout(operation: Operation) =>
      this.logInfo(s"Timeout $operation")
      numberOfTimeouts += 1
      operation match {
        case ReadOperation(key: String, _) =>
          resendRead(key)
        case WriteOperation(key: String, value: String, requestId: String) =>
          resendWrite(key, value, requestId.toLong)
        case _ =>
          log.warning(s"\nClient can't execute operation $operation")
      }

  }
}
