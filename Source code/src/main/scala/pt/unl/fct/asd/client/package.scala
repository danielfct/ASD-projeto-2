package pt.unl.fct.asd

import pt.unl.fct.asd.server.Operation

package object client {

  final case class OperationInfo(operation: Operation, sendTimestamp: Long, responseTimestamp: Long)

}
