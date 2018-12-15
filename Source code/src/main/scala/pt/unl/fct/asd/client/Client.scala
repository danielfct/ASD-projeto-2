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
  if (args.length < 4) {
    println("Usage: \"sbt runMain Client numberOfClients numberOfOperations ip port replica1 replica2 replica3 [replica4, ...]\"")
    System.exit(1)
  }
  val numberOfClients = args(0).toInt
  val numberOfOperations = args(1).toInt
  val hostname = args(2)
  val port = args(3)
  val replicasInfo = args.drop(4)
  val config: Config = buildConfiguration(hostname, port)

  implicit val system: ActorSystem = ActorSystem("Client", config)
  for (i <- 1 to numberOfClients) {
    val client = system.actorOf(Client.props(numberOfOperations, replicasInfo), s"client$i")
    client ! "START"
  }

  def props(numberOfOperations: Int, replicasInfo: Array[String]): Props =
    Props(new Client(numberOfOperations, replicasInfo))
}

class Client(var numberOfOperations: Int, val replicasInfo: Array[String]) extends Actor with ActorLogging {

  val replicas: Set[ActorSelection] = this.selectReplicas(replicasInfo)
  var leader: ActorRef = _
  var requests: Map[OperationInfo, Operation] = Map.empty[OperationInfo, Operation]
  var lastOperation: Operation = _
  var startTime: Long = -1L
  var numberOfReads: Int = 0
  var numberOfWrites: Int = 0
  var numberOfTimeouts: Int = 0
  val OPERATION_TIMEOUT: Int = 5000
  var operationTimeoutSchedule: Cancellable = _
  val r = new Random

  def selectReplicas(replicasInfo: Array[String]): Set[ActorSelection] = {
    var replicas: Set[ActorSelection] = Set.empty[ActorSelection]
    replicasInfo.foreach(replica => replicas += context.actorSelection(s"akka.tcp://Server@$replica"))
    log.info(s"\nInitial replicas: $replicas")
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
    log.info(s"\nResent read operation key=$key")
    randomReplica ! Read(key)
    operationTimeoutSchedule = context.system.scheduler.scheduleOnce(OPERATION_TIMEOUT millis, self, OperationTimeout(lastOperation))
  }

  private def resendWrite(key: String, value: String, timestamp: Long): Unit = {
    log.info(s"\nResent write operation key=$key value=$value")
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
        log.info(s"\nSent read operation key=$key")
      case WriteOperation(key: String, value: String, requestId: String) =>
        val timestamp = requestId.toLong
        requests += OperationInfo(timestamp, -1L) -> lastOperation
        numberOfWrites += 1
        randomReplica ! Write(key, value, timestamp)
        log.info(s"\nSent write operation key=$key value=$value")
    }
    operationTimeoutSchedule = context.system.scheduler.scheduleOnce(OPERATION_TIMEOUT millis, self, OperationTimeout(lastOperation))
  }

  private def processResponse(): Unit = {
    Thread.sleep(1000)
    lastOperation match {
      case ReadOperation(_, requestId: String) =>
        requests = requests.filterKeys(k => !k.equals(OperationInfo(requestId.toLong, -1)))
        requests += OperationInfo(requestId.toLong, System.currentTimeMillis()) -> lastOperation
      case WriteOperation(_, _, requestId: String) =>
        requests = requests.filterKeys(k => !k.equals(OperationInfo(requestId.toLong, -1)))
        requests += OperationInfo(requestId.toLong, System.currentTimeMillis()) -> lastOperation
        leader = sender
    }
  }

  private def end(): Unit = {
    printStats()
  }

  private def printStats(): Unit = {
    val endTime: Long = System.currentTimeMillis()
    val duration: Long = endTime - startTime
    log.info(s"\nStart time: ${new SimpleDateFormat("dd/MM/yyyy hh:mm:ss.SSS").format(new Date(startTime))}\n" +
      s"End time: ${new SimpleDateFormat("dd/MM/yyyy hh:mm:ss.SSS").format(new Date(endTime))}\n" +
      s"Duration: ${duration/1000} seconds\n" +
      s"Requests: $requests\n" +
      s"Number of reads: $numberOfReads\n" +
      s"Number of writes: $numberOfWrites\n" +
      s"Number of timeouts: $numberOfTimeouts\n")
  }

  override def receive: Receive = {
    /* Experiments should report the latency and throughput of the designed system
      (ideally through a latency throughout graph) for different number of concurrent clients.
      Additionally, students might also try to evaluate the effects on
      latency and throughput of replicas failing or joining the system).*/

    case "START" =>
      startTime = System.currentTimeMillis()
      if (numberOfOperations > 0) {
        this.sendOperation()
      } else {
        this.end()
      }

    case Response(result: Option[String]) =>
      log.info(s"\nGot response value=$result\nOperations left: ${numberOfOperations-1}")
      operationTimeoutSchedule.cancel()
      numberOfOperations -= 1
      this.processResponse()
      if (numberOfOperations > 0) {
        this.sendOperation()
      } else {
        this.end()
      }

    case OperationTimeout(operation: Operation) =>
      log.info(s"\nTimeout $operation")
      numberOfTimeouts += 1
      operation match {
        case ReadOperation(key: String, _) =>
          resendRead(key)
        case WriteOperation(key: String, value: String, requestId: String) =>
          resendWrite(key, value, requestId.toLong)
      }

  }
}
