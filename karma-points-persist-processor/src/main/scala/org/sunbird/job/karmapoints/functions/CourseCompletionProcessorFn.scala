package org.sunbird.job.karmapoints.functions

import com.fasterxml.jackson.core.JsonProcessingException
import org.apache.commons.lang3.StringUtils
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

import java.time.{LocalDateTime, Period}
import java.time.format.DateTimeFormatter
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
    val assessmentIdentifier = doesAssessmentExistInHierarchy(hierarchy)(metrics, config)
    if(!StringUtils.isEmpty(assessmentIdentifier)) {
      val assessmentResponse = fetchUserAssessmentResult(userId,assessmentIdentifier)(config, cassandraUtil)
      if(assessmentResponse != null && assessmentResponse.size() >0){
       val result = assessmentResponse.get(0).getString(config.DB_COLUMN_SUBMIT_ASSESSMENT_RESPONSE)
        var pass = java.lang.Boolean.FALSE
        if(!StringUtils.isEmpty(result)){
          val resultMap = JSONUtil.deserialize[java.util.Map[String, Any]](result)
           pass =  resultMap.getOrDefault(config.PASS,java.lang.Boolean.FALSE).asInstanceOf[Boolean]
        }
        if(pass) {
        points = points+config.assessmentQuotaKarmaPoints
        addInfoMap.put(config.ADDINFO_ASSESSMENT_PASS, java.lang.Boolean.TRUE)
       }else {
        addInfoMap.put(config.ADDINFO_ASSESSMENT_PASS, java.lang.Boolean.FALSE)
       }
      }
        addInfoMap.put(config.ADDINFO_ASSESSMENT, java.lang.Boolean.TRUE)
    }
    val headers = Map[String, String](
      config.HEADER_CONTENT_TYPE_KEY -> config.HEADER_CONTENT_TYPE_JSON
      , config.X_AUTHENTICATED_USER_ORGID-> fetchUserRootOrgId(userId)(config, cassandraUtil)
      , config.X_AUTHENTICATED_USER_ID -> userId)
    val result = doesCourseBelongsToACBPPlan(headers)(metrics, config, httpUtil).get(contextId) match { case Some(value) => value case _ => "" }
    if(!StringUtils.isEmpty(result)){
      nonACBPCount = 0
      points = points+config.acbpQuotaKarmaPoints
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
      val inputDate = LocalDateTime.parse(result, formatter)
      val currentDate = LocalDateTime.now
      if(currentDate.isAfter(inputDate)) {
        val period = Period.between(inputDate.toLocalDate,currentDate.toLocalDate)
        val monthsDifference = period.getYears * 12 + period.getMonths + 1
        if(monthsDifference > config.acbpQuotaKarmaPoints)
          points = points-config.acbpQuotaKarmaPoints
        else
          points = points-monthsDifference
      }
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
