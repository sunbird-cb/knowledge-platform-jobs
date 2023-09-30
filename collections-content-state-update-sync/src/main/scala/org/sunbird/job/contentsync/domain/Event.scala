package org.sunbird.job.contentsync.domain

import org.sunbird.job.contentsync.task.CollectionsContentStateSyncConfig
import org.sunbird.job.domain.reader.JobRequest

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long)  extends JobRequest(eventMap, partition, offset) {
    
    def action:String = readOrDefault[String]("edata.action", "")

    def batchId: String = readOrDefault[String]("edata.batchId", "")

    def courseId: String = readOrDefault[String]("edata.courseId", "")

    def userId: String = readOrDefault[String]("edata.userId", "")

    def providerName: String = readOrDefault[String]("edata.providerName", "")

    def primaryCategory: String = readOrDefault[String]("edata.primaryCategory", "")

    def parentCollections: List[String] = readOrDefault[List[String]]("edata.parentCollections", List.empty[String])


    def eData: Map[String, AnyRef] = readOrDefault[Map[String, AnyRef]]("edata", Map[String, AnyRef]())

    def isValid()(config: CollectionsContentStateSyncConfig): Boolean = {
        config.collectionsContentStateSyncProcess.equalsIgnoreCase(action) && !batchId.isEmpty && !courseId.isEmpty &&
          !userId.isEmpty
    }
}
