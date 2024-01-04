package org.sunbird.job.karmapoints.domain

import org.sunbird.job.domain.reader.JobRequest
import org.sunbird.job.karmapoints.task.KarmaPointsProcessorConfig

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long)  extends JobRequest(eventMap, partition, offset) {

    def userId: String = readOrDefault[String]("edata.userId", "")

    def contextType:String = readOrDefault[String]("edata.context_type", "")

    def operationType:String = readOrDefault[String]("edata.operation_type", "")

    def contextId:String = readOrDefault[String]("edata.context_id", "")

    def isValid()(config: KarmaPointsProcessorConfig): Boolean = {
        userId != null && null != operationType && null != contextId  && null != contextType

    }
}
