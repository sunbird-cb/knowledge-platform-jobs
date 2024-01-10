package org.sunbird.job.karmapoints.functions

import com.fasterxml.jackson.core.JsonProcessingException
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.karmapoints.domain.Event
import org.sunbird.job.karmapoints.task.KarmaPointsProcessorConfig
import org.sunbird.job.karmapoints.util.Utility._
import org.sunbird.job.util.{CassandraUtil, HttpUtil, JSONUtil}
import org.sunbird.job.{BaseProcessFunction, Metrics}

import java.util

class CourseCompletionProcessorFn(config: KarmaPointsProcessorConfig, httpUtil: HttpUtil)
                                 (implicit val stringTypeInfo: TypeInformation[String],
                                @transient var cassandraUtil: CassandraUtil = null)
  extends BaseProcessFunction[Event, String](config)   {

  private[this] val logger = LoggerFactory.getLogger(classOf[CourseCompletionProcessorFn])
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
    val eventData = event.getMap().get(config.EDATA).asInstanceOf[scala.collection.immutable.Map[String, Any]]
    val userId = eventData.get(config.USERIDS).asInstanceOf[Option[List[Any]]].get(0).asInstanceOf[String]
    val contextId: String = eventData.get(config.COURSE_ID) match { case Some(value) => value.asInstanceOf[String] case _ => "" }
    val hierarchy: java.util.Map[String, AnyRef] = fetchContentHierarchy(contextId) ( metrics,config, cassandraUtil)
    if (Option(hierarchy).isEmpty || hierarchy.isEmpty) {
      return
    }
    val contextType = hierarchy.get(config.PRIMARY_CATEGORY).asInstanceOf[String]
    if(!passThroughValidation(contextType, contextId, userId, config, cassandraUtil, metrics))
      return
    kpOnCourseCompletion(userId, contextType,config.OPERATION_COURSE_COMPLETION,contextId, hierarchy,config, httpUtil, cassandraUtil)(metrics)
  }
  private def kpOnCourseCompletion(userId : String, contextType : String, operationType:String,
                           contextId:String, hierarchy:java.util.Map[String, AnyRef],
                           config: KarmaPointsProcessorConfig,
                           httpUtil: HttpUtil, cassandraUtil: CassandraUtil)(metrics: Metrics) :Unit = {
    var nonACBPCount = 1
    val addInfoMap = new util.HashMap[String, AnyRef]
    addInfoMap.put(config.ADDINFO_ASSESSMENT, java.lang.Boolean.FALSE)
    addInfoMap.put(config.ADDINFO_ACBP, java.lang.Boolean.FALSE)
    addInfoMap.put(config.OPERATION_COURSE_COMPLETION, java.lang.Boolean.TRUE)
    addInfoMap.put(config.ADDINFO_COURSENAME, hierarchy.get(config.name))
    var points : Int = config.courseCompletionQuotaKarmaPoints
    if(doesAssessmentExistInHierarchy(hierarchy)(metrics,config)) {
      points = points+config.assessmentQuotaKarmaPoints
      addInfoMap.put(config.ADDINFO_ASSESSMENT, java.lang.Boolean.TRUE)
    }
    val headers = Map[String, String](
      config.HEADER_CONTENT_TYPE_KEY -> config.HEADER_CONTENT_TYPE_JSON
      , config.X_AUTHENTICATED_USER_ORGID-> fetchUserRootOrgId(userId)(config, cassandraUtil)
      , config.X_AUTHENTICATED_USER_ID -> userId)
    if(doesCourseBelongsToACBPPlan(contextId, headers)(metrics, config, httpUtil)){
      nonACBPCount = 0
      points = points+config.acbpQuotaKarmaPoints
      addInfoMap.put(config.ADDINFO_ACBP, java.lang.Boolean.TRUE)
    }
    var addInfo = config.EMPTY
    try addInfo = mapper.writeValueAsString(addInfoMap)
    catch {
      case e: JsonProcessingException =>
        throw new RuntimeException(e)
    }
    insertKarmaPoints(userId, contextType,operationType,contextId,points, addInfo)(metrics,config, cassandraUtil)
    processUserKarmaSummaryUpdate(userId, points, nonACBPCount)(config, cassandraUtil)
  }
  private def passThroughValidation(contextType:String, contextId : String, userId: String,
                                    config: KarmaPointsProcessorConfig,
                                    cassandraUtil: CassandraUtil, metrics: Metrics): Boolean = {
    if(!config.COURSE.equals(contextType) ||
      hasReachedNonACBPMonthlyCutOff(userId)(metrics, config, cassandraUtil) ||
      doesEntryExist(userId, contextType, config.OPERATION_COURSE_COMPLETION, contextId)( metrics,config, cassandraUtil))
      return java.lang.Boolean.FALSE
    else
      return java.lang.Boolean.TRUE
  }
}
