package org.sunbird.job.karmapoints.functions

import com.fasterxml.jackson.core.JsonProcessingException
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.sunbird.job.karmapoints.domain.Event
import org.sunbird.job.karmapoints.task.KarmaPointsProcessorConfig
import org.sunbird.job.karmapoints.util.Utility
import org.sunbird.job.util.{CassandraUtil, HttpUtil}
import org.sunbird.job.{BaseProcessFunction, Metrics}

import java.util

class KarmaPointsFirstEnrolmentProcessorFn(config: KarmaPointsProcessorConfig, httpUtil: HttpUtil)
                                          (implicit val stringTypeInfo: TypeInformation[String],
                                @transient var cassandraUtil: CassandraUtil = null)
  extends BaseProcessFunction[Event, String](config)   {
  lazy private val mapper: ObjectMapper = new ObjectMapper()

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
  }

  override def close(): Unit = {
    cassandraUtil.close()
    super.close()
  }

  override def metricsList(): List[String] = {
    List(config.totalEventsCount, config.dbReadCount, config.dbUpdateCount, config.failedEventCount, config.skippedEventCount, config.successEventCount,
      config.cacheHitCount, config.karmaPointsIssueEventsCount, config.cacheMissCount)
  }

  override def processElement(event: Event,
                              context: ProcessFunction[Event, String]#Context,
                              metrics: Metrics): Unit = {
    val eData = event.getMap().get(config.EDATA).asInstanceOf[scala.collection.immutable.Map[String, Any]]
    val userIdSome : Option[Any] = eData.get(config.USER_ID_CAMEL)
    val usrId: String = userIdSome match {
      case Some(value) => value.asInstanceOf[String]
      case None => config.EMPTY
    }
    val courseId : Option[Any] = eData.get(config.COURSE_ID)
    val contextId: String = courseId match {
      case Some(value) => value.asInstanceOf[String]
      case None => config.EMPTY
    }
    val batchId : Option[Any] = eData.get(config.BATCH_ID)
    val batchId_str: String = batchId match {
      case Some(value) => value.asInstanceOf[String]
      case None => config.EMPTY
    }
    val hierarchy: java.util.Map[String, AnyRef] = Utility.fetchContentHierarchy(contextId, config, cassandraUtil)(metrics)
    if(null == hierarchy || hierarchy.size() < 1)
      return
    val contextType = hierarchy.get(config.PRIMARY_CATEGORY).asInstanceOf[String]

    if(Utility.isEntryAlreadyExist(usrId,contextType,config.OPERATION_TYPE_ENROLMENT,contextId,config, cassandraUtil)
      || !Utility.isFirstEnrolment(usrId,config,cassandraUtil))
      return
    firstEnrolment(usrId, contextType,config.OPERATION_TYPE_ENROLMENT,contextId,cassandraUtil)(metrics)
  }
  private def firstEnrolment(userId : String, contextType : String,operationType:String,contextId:String, cassandraUtil: CassandraUtil)(metrics: Metrics) :Unit = {
    val points: Int = config.firstEnrolmentQuotaKarmaPoints
    val addInfoMap = new util.HashMap[String, AnyRef]
    val hierarchy: java.util.Map[String, AnyRef] = Utility.fetchContentHierarchy(contextId, config, cassandraUtil)(metrics)
    if(null == hierarchy || hierarchy.size() < 1)
      return
    addInfoMap.put(config.ADDINFO_COURSENAME, hierarchy.get(config.name))
    var addInfo = config.EMPTY
    try addInfo = mapper.writeValueAsString(addInfoMap)
    catch {
      case e: JsonProcessingException =>
        throw new RuntimeException(e)
    }
    Utility.insertKarmaPoints(userId, contextType,operationType,contextId,points,addInfo,config,cassandraUtil)(metrics)
    Utility.updateKarmaSummary(userId, points, config, cassandraUtil)

  }
}
