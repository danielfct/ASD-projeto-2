package pt.unl.fct.asd

import pt.unl.fct.asd.server.Operation

package object client {

  final case class OperationInfo(operation: Operation, sendTimestamp: Long, responseTimestamp: Long)

  final class Stats(val endTime: Long, val durationMillis: Long, val durationSeconds: Float,
                    val requestsPerSecond: Float, val averageLatency: Int, val averageReadsLatency: Int,
                    val averageWritesLatency: Int)

}
