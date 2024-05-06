package org.sunbird.job.karmapoints.functions

import com.fasterxml.jackson.core.JsonProcessingException
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.karmapoints.domain.Event
import org.sunbird.job.karmapoints.task.KarmaPointsProcessorConfig
import org.sunbird.job.karmapoints.util.Utility.{mapper, _}
import org.sunbird.job.util.{CassandraUtil, HttpUtil, JSONUtil}
import org.sunbird.job.{BaseProcessFunction, Metrics}
import java.util.Date
import scala.collection.convert.ImplicitConversions.`map AsScala`

class ClaimACBPProcessorFn(config: KarmaPointsProcessorConfig, httpUtil: HttpUtil)
                          (implicit val stringTypeInfo: TypeInformation[String],
                                         @transient var cassandraUtil: CassandraUtil = null)
  extends BaseProcessFunction[Event, String](config) {
  private[this] val logger = LoggerFactory.getLogger(classOf[ClaimACBPProcessorFn])

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
    val eventData = event.getMap().get(config.EDATA).asInstanceOf[scala.collection.immutable.Map[String, Any]]
    val userId: String = eventData.getOrElse(config.USER_ID_CAMEL, "").asInstanceOf[String]
    val contextId: String = eventData.getOrElse(config.COURSE_ID, "").asInstanceOf[String]
    claimACBPKarmaPoints(userId, config.OPERATION_COURSE_COMPLETION, contextId, config, cassandraUtil)(metrics)
  }

  private def claimACBPKarmaPoints(userId: String, operationType: String, contextId: String,
                        config: KarmaPointsProcessorConfig, cassandraUtil: CassandraUtil)(metrics: Metrics): Unit = {
    val headers = Map(
      config.HEADER_CONTENT_TYPE_KEY -> config.HEADER_CONTENT_TYPE_JSON,
      config.X_AUTHENTICATED_USER_ORGID -> fetchUserRootOrgId(userId)(config, cassandraUtil),
      config.X_AUTHENTICATED_USER_ID -> userId
    )
    if (!doesCourseBelongsToACBPPlan(contextId, headers)(metrics, config, httpUtil)) {
      logger.info(s"Request is not part of ACBP for userId: $userId, courseId: $contextId")
      return
    }
    val hierarchy : java.util.Map[String, AnyRef] = fetchContentHierarchy(contextId) (metrics,config, cassandraUtil)
    if (hierarchy == null || hierarchy.size() < 1) {
      return
    }
    val contextType = hierarchy.get(config.PRIMARY_CATEGORY).asInstanceOf[String]
    if (config.COURSE != contextType) {
      return
    }
    val courseName = Option(hierarchy.get(config.name).asInstanceOf[String]).map(_.replace(",", "\\,")).getOrElse("")
    val res = fetchUserKarmaPointsCreditLookup(userId, contextType, config.OPERATION_COURSE_COMPLETION, contextId)(config, cassandraUtil)
    if (res == null || res.isEmpty) {
      logger.info(s"Making new entry for ACBP with userId: $userId, courseId: $contextId")
      val addInfoMap = Map(
        config.ADDINFO_ACBP -> java.lang.Boolean.TRUE,
        config.OPERATION_COURSE_COMPLETION -> java.lang.Boolean.FALSE,
        config.ADDINFO_COURSENAME -> courseName
      )
      val addInfo = try mapper.writeValueAsString(addInfoMap) catch {
        case e: JsonProcessingException =>
          logger.error("Error serializing addInfoMap", e)
          return
      }
      insertKarmaPoints(userId, contextType, operationType, contextId, config.acbpQuotaKarmaPoints, addInfo)(metrics, config, cassandraUtil)
      processUserKarmaSummaryUpdate(userId, config.acbpQuotaKarmaPoints, -1)(config, cassandraUtil)
    } else {
      logger.info(s"Updating entry for ACBP with userId: $userId, courseId: $contextId")
      val creditDate = res.get(0).getObject(config.DB_COLUMN_CREDIT_DATE).asInstanceOf[Date]
      val entry = fetchUserKarmaPoints(creditDate, userId, contextType, config.OPERATION_COURSE_COMPLETION, contextId)(config, cassandraUtil)
      var points = entry.get(0).getInt(config.POINTS)
      val addInfo = entry.get(0).getString(config.ADD_INFO)
      val addInfoMap = JSONUtil.deserialize[java.util.Map[String, Any]](addInfo)
      if (addInfoMap.getOrElse(config.ADDINFO_ACBP, java.lang.Boolean.FALSE.asInstanceOf[Boolean]).asInstanceOf[Boolean]) {
        return
      }
      addInfoMap(config.ADDINFO_ACBP) = java.lang.Boolean.TRUE
      val addInfoStr = try mapper.writeValueAsString(addInfoMap) catch {
        case e: JsonProcessingException =>
          logger.error("Error serializing addInfoMap", e)
          return
      }
      points = points + config.acbpQuotaKarmaPoints
      updatePoints(userId, contextType, operationType, contextId, points, addInfoStr, creditDate.getTime)(config, cassandraUtil)
      processUserKarmaSummaryUpdate(userId, config.acbpQuotaKarmaPoints, -1)(config, cassandraUtil)
    }
  }
}