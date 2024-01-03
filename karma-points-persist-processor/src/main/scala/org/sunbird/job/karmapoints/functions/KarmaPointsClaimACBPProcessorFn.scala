package org.sunbird.job.karmapoints.functions

import com.datastax.driver.core.Row
import com.fasterxml.jackson.core.JsonProcessingException
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.sunbird.job.karmapoints.domain.Event
import org.sunbird.job.karmapoints.task.KarmaPointsProcessorConfig
import org.sunbird.job.karmapoints.util.Utility
import org.sunbird.job.util.{CassandraUtil, HttpUtil, JSONUtil}
import org.sunbird.job.{BaseProcessFunction, BaseProcessKeyedFunction, Metrics}

import java.util
import java.util.Date

class KarmaPointsClaimACBPProcessorFn(config: KarmaPointsProcessorConfig, httpUtil: HttpUtil)
                                     (implicit val stringTypeInfo: TypeInformation[String],
                                @transient var cassandraUtil: CassandraUtil = null)
  extends BaseProcessFunction[Event, String](config)    {

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
    List(config.totalEventsCount, config.dbReadCount, config.dbUpdateCount,
      config.failedEventCount,
      config.skippedEventCount, config.successEventCount,
      config.cacheHitCount, config.karmaPointsIssueEventsCount, config.cacheMissCount)
  }

  override def processElement(event: Event,
                              context: ProcessFunction[Event, String]#Context,
                              metrics: Metrics): Unit = {
    val eData = event.getMap().get(config.EDATA).asInstanceOf[scala.collection.immutable.Map[String, Any]]
    val userIdSome: Option[Any] = eData.get(config.USER_ID_CAMEL)
    val usrId: String = userIdSome match {
      case Some(value) => value.asInstanceOf[String]
      case None => ""
    }
    val courseId: Option[Any] = eData.get(config.COURSE_ID)
    val contextId: String = courseId match {
      case Some(value) => value.asInstanceOf[String]
      case None => ""
    }

    val hierarchy: java.util.Map[String, AnyRef] = Utility.fetchContentHierarchy(contextId, config, cassandraUtil)(metrics)
    val contextType = hierarchy.get(config.PRIMARY_CATEGORY).asInstanceOf[String]
    val res = Utility.karmaPointslookup(usrId,contextType,config.OPERATION_COURSE_COMPLETION,contextId,config,cassandraUtil)
    if(res == null || res.isEmpty)
      Utility.courseCompletion(usrId, contextType,config.OPERATION_COURSE_COMPLETION,contextId, hierarchy,config,httpUtil,cassandraUtil)(metrics)
    else
      claimACBP(usrId , config.OPERATION_COURSE_COMPLETION,contextId,config,cassandraUtil,res,contextType)(metrics)
  }

  private def claimACBP(  usrId : String,operationType:String,contextId:String
                        ,config: KarmaPointsProcessorConfig,cassandraUtil: CassandraUtil,res:util.List[Row],contextType:String) (metrics: Metrics):Unit = {
    val headers = Map[String, String](
      "Content-Type" -> "application/json"
      , "x-authenticated-user-orgid" -> Utility.userRootOrgId(usrId, config, cassandraUtil)
      , "x-authenticated-userid" -> usrId)

    if(!Utility.isACBP(contextId,httpUtil,config,headers)(metrics))
      return
    val credit_date = res.get(0).getObject(config.DB_COLUMN_CREDIT_DATE).asInstanceOf[Date]
    val entry = Utility.karmaPointsEntry(credit_date,usrId,contextType,config.OPERATION_COURSE_COMPLETION,contextId,config,cassandraUtil)
    var points= entry.get(0).getInt(config.POINTS)
    val addInfo= entry.get(0).getString(config.ADD_INFO)
    val addInfoMap = JSONUtil.deserialize[java.util.Map[String, Any]](addInfo)
    addInfoMap.put(config.ADDINFO_ACBP, java.lang.Boolean.TRUE)
    var addInfoStr = ""
    try addInfoStr = mapper.writeValueAsString(addInfoMap)
    catch {
      case e: JsonProcessingException =>
        throw new RuntimeException(e)
    }
    points = points+config.acbpQuotaKarmaPoints
    Utility.upsertKarmaPoints(usrId, contextType ,operationType,contextId,points,addInfoStr,credit_date.getTime,config, cassandraUtil)
  }
}
