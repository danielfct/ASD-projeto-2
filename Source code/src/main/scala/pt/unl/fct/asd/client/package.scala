package pt.unl.fct.asd

import pt.unl.fct.asd.server.Operation

package object client {

  final case class OperationTimeout(operation: Operation)

  final case class OperationInfo(sendTimestamp: Long, responseTimestamp: Long) {
    def canEqual(a: Any): Boolean = a.isInstanceOf[OperationInfo]
    override def equals(that: Any): Boolean =
      that match {
        case that: OperationInfo => that.canEqual(this) && this.hashCode == that.hashCode
        case _ => false
      }
    override def hashCode: Int = {
      31 + sendTimestamp.hashCode
    }
  }

}
