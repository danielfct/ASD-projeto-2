package pt.unl.fct.asd

import pt.unl.fct.asd.server.Operation

package object client {

  final case class OperationTimeout(operation: Operation)

  final case class OperationInfo(sendTimestamp: Long, responseTimestamp: Long)

}
