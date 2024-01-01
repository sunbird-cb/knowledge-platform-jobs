package org.sunbird.job.karmapoints.functions

import com.fasterxml.jackson.core.JsonProcessingException
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.streaming.api.functions.{ProcessFunction}
import org.slf4j.LoggerFactory
import org.sunbird.job.karmapoints.domain.Event
import org.sunbird.job.karmapoints.task.KarmaPointsProcessorConfig
import org.sunbird.job.karmapoints.util.Utility
import org.sunbird.job.util.{CassandraUtil, HttpUtil}
import org.sunbird.job.{BaseProcessFunction,  Metrics}

import java.util

class KarmaPointsCourseCompletionProcessorFn(config: KarmaPointsProcessorConfig, httpUtil: HttpUtil)
                                            (implicit val stringTypeInfo: TypeInformation[String],
                                @transient var cassandraUtil: CassandraUtil = null)
  extends BaseProcessFunction[Event, String](config)   {

  private[this] val logger = LoggerFactory.getLogger(classOf[KarmaPointsCourseCompletionProcessorFn])
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
    val usrIdOption = event.getMap().get(config.EDATA).asInstanceOf[scala.collection.immutable.HashMap[String, Any]]
      .get(config.USERIDS).asInstanceOf[Option[List[Any]]].get(0).asInstanceOf[String]
    val courseId : Option[Any] = event.getMap().get(config.EDATA).asInstanceOf[scala.collection.immutable.HashMap[String, Any]]
      .get(config.COURSE_ID)
    val contextId: String = courseId match {
      case Some(value) => value.asInstanceOf[String]
      case None => ""
    }
    val hierarchy: java.util.Map[String, AnyRef] = Utility.fetchContentHierarchy(contextId, config, cassandraUtil)(metrics)
    val contextType = hierarchy.get(config.PRIMARY_CATEGORY).asInstanceOf[String] // Replace YourTypeForPrimaryCategory with the actual type
    if (Utility.isEntryAlreadyExist(usrIdOption, contextType, config.OPERATION_COURSE_COMPLETION, contextId, config, cassandraUtil))
      return
    courseCompletion(usrIdOption, contextType,config.OPERATION_COURSE_COMPLETION,contextId, hierarchy)(metrics)
  }
  private def courseCompletion(userId : String, contextType : String,operationType:String,contextId:String,hierarchy:java.util.Map[String, AnyRef])(metrics: Metrics) :Unit = {
    var points : Int = config.courseCompletionPoints
    val addInfoMap = new util.HashMap[String, AnyRef]
    addInfoMap.put(config.ADDINFO_ASSESSMENT, java.lang.Boolean.FALSE)
    addInfoMap.put(config.ADDINFO_ACBP, java.lang.Boolean.FALSE)
    addInfoMap.put(config.ADDINFO_COURSENAME, hierarchy.get(config.name))
    if(Utility.isAssessmentExist(hierarchy,config)(metrics)) {
      points = points+config.assessmentQuotaKarmaPoints
      addInfoMap.put(config.ADDINFO_ASSESSMENT, java.lang.Boolean.TRUE)
    }
    val headers = Map[String, String](
       "Content-Type" -> "application/json"
      ,"x-authenticated-user-orgid"->Utility.userRootOrgId(userId,config, cassandraUtil)
      ,"x-authenticated-userid"->userId)

    if(Utility.isACBP(contextId,httpUtil,config,headers)(metrics)){
      points = points+config.acbpQuotaKarmaPoints
      addInfoMap.put(config.ADDINFO_ACBP, java.lang.Boolean.TRUE)
    }
    var addInfo = ""
    try addInfo = mapper.writeValueAsString(addInfoMap)
    catch {
      case e: JsonProcessingException =>
        throw new RuntimeException(e)
    }
    Utility.insertKarmaPoints(userId, contextType,operationType,contextId,points, addInfo,config, cassandraUtil)(metrics)
  }
}
