package org.sunbird.job.karmapoints.functions

import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.{Insert, QueryBuilder, Select}
import com.datastax.driver.core.utils.UUIDs
import com.fasterxml.jackson.core.JsonProcessingException
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.karmapoints.domain.Event
import org.sunbird.job.karmapoints.task.KarmaPointsProcessorConfig
import org.sunbird.job.util.{CassandraUtil, HttpUtil}
import org.sunbird.job.{BaseProcessKeyedFunction, Metrics}
import java.util
import java.util.UUID
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

class KarmaPointsProcessorFn(config: KarmaPointsProcessorConfig, httpUtil: HttpUtil)
                            (implicit val stringTypeInfo: TypeInformation[String],
                                @transient var cassandraUtil: CassandraUtil = null)
  extends BaseProcessKeyedFunction[String, Event, String](config)  {

  private[this] val logger = LoggerFactory.getLogger(classOf[KarmaPointsProcessorFn])

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
                              context: KeyedProcessFunction[String, Event, String]#Context,
                              metrics: Metrics): Unit = {

   event.operationType match {
      case config.courseCompletionOperationType => courseCompletion(event)(metrics)
      case config.ratingOperationType => rating(event)(metrics)
      case config.firstEnrolmentOperationType => firstEnrolment(event)(metrics)
      case config.firstLoginOperationType => firstLogin(event)(metrics)
   }
  }

  private def courseCompletion(event : Event)(metrics: Metrics) :Unit = {
    var points : Int = 5
    val addInfoMap = new util.HashMap[String, AnyRef]
    addInfoMap.put("ASSESSMENT", java.lang.Boolean.FALSE)
    addInfoMap.put("ACBP", java.lang.Boolean.FALSE)
    if(isAssessmentExist(event.contextId)(metrics)) {
      points = points+5
      addInfoMap.put("ASSESSMENT", java.lang.Boolean.TRUE)
    }
    if(isACBP(event.contextId)(metrics)){
      points = points+10
      addInfoMap.put("ACBP", java.lang.Boolean.TRUE)
    }
    var addInfo = ""
    try addInfo = mapper.writeValueAsString(addInfoMap)
    catch {
      case e: JsonProcessingException =>
        throw new RuntimeException(e)
    }
    insertKarmaPoints(event,points, addInfo)(metrics)
  }

  private def rating(event : Event)(metrics: Metrics) :Unit = {
    val points: Int = 2
    insertKarmaPoints(event,points)(metrics)
  }

  private def firstEnrolment(event : Event)(metrics: Metrics) :Unit = {
    val points: Int = 5
    insertKarmaPoints(event,points)(metrics)
  }

  private def firstLogin(event : Event)(metrics: Metrics) :Unit = {
    val points: Int = 5
    insertKarmaPoints(event,points)(metrics)
  }

  private def insertKarmaPoints(event : Event, points:Int)(implicit metrics: Metrics): Unit = {
     insertKarmaPoints(event, points,"")(metrics)
  }

  private def insertKarmaPoints(event : Event, points:Int, addInfo:String)(implicit metrics: Metrics): Unit = {
    val query: Insert = QueryBuilder.insertInto(config.sunbird_keyspace, config.user_karma_points_table)
    val uid = UUIDs.timeBased
    query.value("userid", event.userId)
    query.value("credit_date",  uid)
    query.value("context_type", event.contextType)
    query.value("operation_type", event.operationType)
    query.value("context_id", event.contextId)
    query.value("addinfo", addInfo)
    query.value("points",points)
    val result = cassandraUtil.upsert(query.toString)
    if (result) {
       insertKarmaCreditLookUp(event, uid)
      metrics.incCounter(config.dbUpdateCount)
    } else {
      val msg = "Database update has failed" + query.toString
      logger.error(msg)
      throw new Exception(msg)
    }
  }

  private def insertKarmaCreditLookUp(event: Event, uid: UUID): Boolean = {
    val query: Insert = QueryBuilder.insertInto(config.sunbird_keyspace, config.user_karma_points_credit_lookup_table)
    query.value("user_karma_points_key", event.userId + "_"+event.contextType+"_"+ event.contextId)
    query.value("operation_type", event.operationType)
    query.value("credit_date", uid)
    cassandraUtil.upsert(query.toString)
  }

  private def isAssessmentExist(courseId: String)(implicit metrics: Metrics):Boolean = {
    val courseList = fetchContentHierarchy(courseId)(metrics)
    var result = false
    if (null != courseList) {
      val hierarchy = courseList.get(0).getString("hierarchy")
      val childrenMap = mapper.readValue(hierarchy, classOf[java.util.Map[String, AnyRef]]).get("children").asInstanceOf[util.ArrayList[util.HashMap[String, AnyRef]]];
      for (children <- childrenMap) {
        val primaryCategory = children.get("primaryCategory")
        if (primaryCategory == "Course Assessment") result = true
      }
    }
    result
  }
  private def isACBP(courseId: String)(implicit metrics: Metrics):Boolean = {
    true
  }
  private def fetchContentHierarchy(courseId: String)(implicit metrics: Metrics): java.util.List[Row] = {
    val selectWhere: Select.Where = QueryBuilder.select(config.Hierarchy)
      .from(config.content_hierarchy_KeySpace, config.content_hierarchy_table).where()
    selectWhere.and(QueryBuilder.eq(config.identifier, courseId))
    metrics.incCounter(config.dbReadCount)
    cassandraUtil.find(selectWhere.toString)
  }
}
