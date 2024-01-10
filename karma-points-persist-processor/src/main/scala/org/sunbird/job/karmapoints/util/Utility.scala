package org.sunbird.job.karmapoints.util

import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.{Insert, QueryBuilder, Select}
import com.fasterxml.jackson.core.JsonProcessingException
import org.apache.commons.lang3.StringUtils
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.sunbird.job.karmapoints.task.KarmaPointsProcessorConfig
import org.sunbird.job.util.{CassandraUtil, HttpUtil, JSONUtil, ScalaJsonUtil}
import org.sunbird.job.Metrics

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util
import java.util.Date
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

object Utility {
  private val logger = LoggerFactory.getLogger("org.sunbird.job.karmapoints.util.Utility")
  lazy private val mapper: ObjectMapper = new ObjectMapper()

   def insertKarmaPoints(userId : String, contextType : String,operationType:String,contextId:String, points:Int,config: KarmaPointsProcessorConfig,cassandraUtil: CassandraUtil)(implicit metrics: Metrics): Unit = {
    insertKarmaPoints(userId, contextType,operationType,contextId, points,"")(metrics,config, cassandraUtil)
  }
  def updatePoints( userId: String,
                    contextType: String,
                    operationType: String,
                    contextId: String,
                    points: Int,
                    addInfo: String,
                    creditDate: Long
                   )(implicit
  config: KarmaPointsProcessorConfig,
  cassandraUtil: CassandraUtil): Boolean = {
    val query: Insert = QueryBuilder
      .insertInto(config.sunbird_keyspace, config.user_karma_points_table)
      .value(config.USER_ID, userId)
      .value(config.CREDIT_DATE, creditDate)
      .value(config.CONTEXT_TYPE, contextType)
      .value(config.OPERATION_TYPE, operationType)
      .value(config.CONTEXT_ID, contextId)
      .value(config.ADD_INFO, addInfo)
      .value(config.POINTS, points)
    cassandraUtil.upsert(query.toString)
  }

  def insertKarmaPoints( userId: String,
                         contextType: String,
                         operationType: String,
                         contextId: String,
                         points: Int,
                         addInfo: String
                       )(implicit metrics: Metrics,
                         config: KarmaPointsProcessorConfig,
                         cassandraUtil: CassandraUtil): Unit = {
    val creditDate = System.currentTimeMillis()
    val result = updatePoints(userId, contextType, operationType, contextId, points, addInfo, creditDate)( config, cassandraUtil)
    if (result) {
      insertKarmaCreditLookup(userId, contextType, operationType, contextId, creditDate)(config, cassandraUtil)
      metrics.incCounter(config.dbUpdateCount)
    } else {
      val msg = s"Database update has failed for userId: $userId, contextType: $contextType, operationType: $operationType, contextId: $contextId, Points: $points"
      logger.error(msg)
      throw new Exception(msg)
    }
  }

  def doesEntryExist(userId: String, contextType: String, operationType: String, contextId: String)(implicit metrics: Metrics, config: KarmaPointsProcessorConfig, cassandraUtil: CassandraUtil): Boolean = {
    val karmaPointsLookUp = fetchUserKarmaPointsCreditLookup(userId, contextType, operationType, contextId)(config, cassandraUtil)
    if (karmaPointsLookUp.size() < 1)
      return false
    val creditDate = karmaPointsLookUp.get(0).getObject(config.DB_COLUMN_CREDIT_DATE).asInstanceOf[Date]
    val result = fetchUserKarmaPoints(creditDate, userId, contextType, operationType, contextId)( config, cassandraUtil)
    result.size() > 0
  }
  def fetchUserKarmaPoints( creditDate: Date,
                            userId: String,
                            contextType: String,
                            operationType: String,
                            contextId: String
                          )(implicit config: KarmaPointsProcessorConfig, cassandraUtil: CassandraUtil): util.List[Row] = {
    val karmaQuery: Select = QueryBuilder
      .select()
      .from(config.sunbird_keyspace, config.user_karma_points_table)
    karmaQuery.where(
        QueryBuilder.eq(config.DB_COLUMN_USERID, userId))
        .and(QueryBuilder.eq(config.DB_COLUMN_CREDIT_DATE, creditDate))
        .and(QueryBuilder.eq(config.DB_COLUMN_CONTEXT_TYPE, contextType))
        .and(QueryBuilder.eq(config.DB_COLUMN_OPERATION_TYPE, operationType))
        .and(QueryBuilder.eq(config.DB_COLUMN_CONTEXT_ID, contextId))
    cassandraUtil.find(karmaQuery.toString)
  }

  def isUserFirstEnrollment(userId: String)(implicit config: KarmaPointsProcessorConfig, cassandraUtil: CassandraUtil): Boolean = {
    val enrollmentLookupQuery: Select = QueryBuilder.select()
      .from(config.sunbird_courses_keyspace, config.user_enrollments_lookup_table)
       enrollmentLookupQuery.where(QueryBuilder.eq(config.DB_COLUMN_USERID, userId))
       cassandraUtil.find(enrollmentLookupQuery.toString).size() < 2
  }

  def fetchUserRootOrgId(userId: String)(implicit config: KarmaPointsProcessorConfig, cassandraUtil: CassandraUtil): String = {
    val userLookupQuery: Select = QueryBuilder
      .select(config.ROOT_ORG_ID)
      .from(config.sunbird_keyspace, config.user_table)
    userLookupQuery.where(QueryBuilder.eq(config.ID, userId))
    val userRow: Row = cassandraUtil.find(userLookupQuery.toString).get(0)
    userRow.getString(config.ROOT_ORG_ID)
  }
  def fetchUserKarmaPointsCreditLookup( userId: String,
                                        contextType: String,
                                        operationType: String,
                                        contextId: String)(implicit
                                        config: KarmaPointsProcessorConfig,
                                        cassandraUtil: CassandraUtil
                                      ): util.List[Row] = {
    val karmaPointsLookupQuery: Select = QueryBuilder
      .select()
      .from(config.sunbird_keyspace, config.user_karma_points_credit_lookup_table)
       karmaPointsLookupQuery.where(QueryBuilder.eq(config.DB_COLUMN_USER_KARMA_POINTS_KEY, userId + config.PIPE + contextType + config.PIPE + contextId)
      ).and(QueryBuilder.eq(config.DB_COLUMN_OPERATION_TYPE, operationType))
    cassandraUtil.find(karmaPointsLookupQuery.toString)
  }

  def countUserKarmaPointsCreditLookup(userId: String, contextType: String,
                                       operationType: String, contextId: String,
                                       config: KarmaPointsProcessorConfig,
                                       cassandraUtil: CassandraUtil): util.List[Row] = {
    val karmaPointsLookupQuery: Select.Where = QueryBuilder
      .select()
      .countAll()
      .from(config.sunbird_keyspace, config.user_karma_points_credit_lookup_table)
      .where(QueryBuilder.eq(config.DB_COLUMN_USER_KARMA_POINTS_KEY, userId + config.PIPE + contextType + config.PIPE + contextId))
        .and(QueryBuilder.eq(config.DB_COLUMN_OPERATION_TYPE, operationType))
    cassandraUtil.find(karmaPointsLookupQuery.toString)
  }

  def doesAssessmentExistInHierarchy(hierarchy: java.util.Map[String, AnyRef])(implicit metrics: Metrics, config: KarmaPointsProcessorConfig): Boolean = {
    var result: Boolean = false
    val childrenMap = hierarchy.get(config.CHILDREN).asInstanceOf[util.ArrayList[util.HashMap[String, AnyRef]]]
    for (children <- childrenMap) {
      val primaryCategory = children.get(config.PRIMARY_CATEGORY)
      if (primaryCategory == config.COURSE_ASSESSMENT) {
        result = true
      }
    }
    result
  }
  def fetchContentHierarchy(courseId: String)(implicit metrics: Metrics,config: KarmaPointsProcessorConfig, cassandraUtil: CassandraUtil): util.HashMap[String, AnyRef] = {
    val selectWhere: Select.Where = QueryBuilder
      .select(config.HIERARCHY)
      .from(config.content_hierarchy_KeySpace, config.content_hierarchy_table)
      .where(QueryBuilder.eq(config.IDENTIFIER, courseId))
    metrics.incCounter(config.dbReadCount)
    val courseList = cassandraUtil.find(selectWhere.toString)
    if (courseList != null && courseList.size() > 0) {
      val hierarchy = courseList.get(0).getString(config.HIERARCHY)
      mapper.readValue(hierarchy, classOf[java.util.Map[String, AnyRef]]).asInstanceOf[util.HashMap[String, AnyRef]]
    } else {
      new util.HashMap[String, AnyRef]()
    }
  }

  def updateKarmaSummary(userId:String,points:Int)(implicit config:KarmaPointsProcessorConfig,cassandraUtil: CassandraUtil): Unit = {
    var total_points:Int = 0
    val userKarmaSummary = fetchUserKpSummary(userId)(config, cassandraUtil)
    if(userKarmaSummary.size() > 0) {
      total_points = userKarmaSummary.get(0).getInt(config.TOTAL_POINTS)
    }
    updateUserKarmaPointsSummary(userId: String, total_points + points,null)(config: KarmaPointsProcessorConfig, cassandraUtil: CassandraUtil)
  }
  def hasReachedNonACBPMonthlyCutOff(userId: String )(implicit metrics: Metrics,config: KarmaPointsProcessorConfig,cassandraUtil: CassandraUtil): Boolean = {
    var infoMap = new util.HashMap[String, Any]()
    val currentDate = LocalDate.now
    val formatter = DateTimeFormatter.ofPattern(config.YYYY_PIPE_MM)
    val currentDateStr = currentDate.format(formatter)
    val userKarmaSummary = fetchUserKpSummary(userId)( config, cassandraUtil)
    var nonACBPCourseQuotaCount: Int = 0
    if (userKarmaSummary.size() > 0) {
      val info = userKarmaSummary.get(0).getString(config.ADD_INFO)
      if (!StringUtils.isEmpty(info)) {
        infoMap = JSONUtil.deserialize[java.util.HashMap[String, Any]](info)
        val currStr = infoMap.get(config.FORMATTED_MONTH)
        if (currentDateStr.equals(currStr)) {
          nonACBPCourseQuotaCount = infoMap.get(config.CLAIMED_NON_ACBP_COURSE_KARMA_QUOTA).asInstanceOf[Int]
        }
      }
    }
    nonACBPCourseQuotaCount >= config.nonAcbpCourseQuota
  }
  private def fetchUserKpSummary(userId: String)(implicit config: KarmaPointsProcessorConfig, cassandraUtil: CassandraUtil): util.List[Row] = {
    val karmaQuery: Select = QueryBuilder.select().from(config.sunbird_keyspace, config.user_karma_summary_table)
    karmaQuery.where(QueryBuilder.eq(config.DB_COLUMN_USERID, userId))
    cassandraUtil.find(karmaQuery.toString)
  }
  private def updateUserKarmaPointsSummary(userId: String, points: Int, addInfo: String)(implicit  config: KarmaPointsProcessorConfig, cassandraUtil: CassandraUtil): Unit = {
    val query: Insert = QueryBuilder
      .insertInto(config.sunbird_keyspace, config.user_karma_summary_table)
      .value(config.USER_ID, userId)
      .value(config.TOTAL_POINTS, points)
    if (addInfo != null)
      query.value(config.ADD_INFO, addInfo)
    cassandraUtil.upsert(query.toString)
  }

  private def executeHttpGetRequest(url: String, headers: Map[String, String])(
    config: KarmaPointsProcessorConfig,
    httpUtil: HttpUtil,
    metrics: Metrics
  ): Map[String, AnyRef] = {
    val response = httpUtil.get(url, headers)
    if (response.status == 200) {
      ScalaJsonUtil
        .deserialize[Map[String, AnyRef]](response.body)
        .getOrElse(config.RESULT, Map.empty[String, AnyRef])
        .asInstanceOf[Map[String, AnyRef]]
    } else if (response.status == 400 && response.body.contains(config.userAccBlockedErrCode)) {
      metrics.incCounter(config.skippedEventCount)
      logger.error(s"Error while fetching user details for $url: ${response.status} :: ${response.body}")
      Map.empty[String, AnyRef]
    } else {
      throw new Exception(s"Error from get API: $url, with response: $response")
    }
  }

   def doesCourseBelongsToACBPPlan(courseId: String, headers: Map[String, String])(
    metrics: Metrics,
    config: KarmaPointsProcessorConfig,
    httpUtil: HttpUtil
  ): Boolean = {
    val apiUrl = config.cbPlanReadUser
    val response = executeHttpGetRequest(apiUrl, headers)(config, httpUtil, metrics)
    val identifiers: List[AnyRef] = response.getOrElse(config.CONTENT, List.empty[AnyRef]) match {
      case content: List[Map[String, AnyRef]] =>
        content.flatMap { contentItem =>
          contentItem.getOrElse(config.CONTENT_LIST, List.empty[AnyRef]) match {
            case contentList: List[Map[String, AnyRef]] =>
              contentList.flatMap(_.get(config.IDENTIFIER))
            case _ =>
              List.empty[AnyRef] // or handle the case when "contentList" is not present in the response
          }
        }
      case _ =>
        List.empty[AnyRef] // or handle the case when "content" is not present in the response
    }
    identifiers.contains(courseId)
  }

  def doesCourseBelongsToACBPPlan(headers: Map[String, String])(
    metrics: Metrics,
    config: KarmaPointsProcessorConfig,
    httpUtil: HttpUtil
  ): Map[String, String] = {
    val apiUrl = config.cbPlanReadUser
    val response = executeHttpGetRequest(apiUrl, headers)(config, httpUtil, metrics)
    response.getOrElse(config.CONTENT, List.empty[AnyRef]) match {
      case content: List[Map[String, AnyRef]] =>
        content.flatMap { contentItem =>
          contentItem.getOrElse(config.CONTENT_LIST, List.empty[AnyRef]) match {
            case contentList: List[Map[String, AnyRef]] =>
              contentList
                .flatMap(item => item.get(config.IDENTIFIER).map(identifier => (identifier.toString, contentItem.getOrElse("endDate", "").toString)))
            case _ =>
              List.empty[(String, String)] // or handle the case when "contentList" is not present in the response
          }
        }.toMap
      case _ =>
        Map.empty[String, String] // or handle the case when "content" is not present in the response
    }
  }

  private def insertKarmaCreditLookup(
                                       userId: String,
                                       contextType: String,
                                       operationType: String,
                                       contextId: String,
                                       creditDate: Long
                                     )(implicit config: KarmaPointsProcessorConfig,
     cassandraUtil: CassandraUtil): Boolean = {
    val karmaCreditLookupQuery: Insert = QueryBuilder
      .insertInto(config.sunbird_keyspace, config.user_karma_points_credit_lookup_table)
      .value(config.DB_COLUMN_USER_KARMA_POINTS_KEY, userId + config.PIPE + contextType + config.PIPE + contextId)
      .value(config.DB_COLUMN_OPERATION_TYPE, operationType)
      .value(config.DB_COLUMN_CREDIT_DATE, creditDate)
    cassandraUtil.upsert(karmaCreditLookupQuery.toString)
  }

  def processUserKarmaSummaryUpdate(userId: String, points: Int, nonACBPQuota : Int)( implicit
                                    config: KarmaPointsProcessorConfig, cassandraUtil: CassandraUtil): Unit = {
    var totalPoints: Int = 0
    var infoMap = new util.HashMap[String, Any]()
    var nonACBPCourseQuotaCount: Int = 0
    val currentDate = LocalDate.now
    val formatter = DateTimeFormatter.ofPattern(config.YYYY_PIPE_MM)
    val currentDateStr = currentDate.format(formatter)
    val userKarmaSummary = fetchUserKpSummary(userId)( config, cassandraUtil)
    if (userKarmaSummary.size() > 0) {
      totalPoints = userKarmaSummary.get(0).getInt(config.TOTAL_POINTS)
      val info = userKarmaSummary.get(0).getString(config.ADD_INFO)
      if (!StringUtils.isEmpty(info)) {
        infoMap = JSONUtil.deserialize[java.util.HashMap[String, Any]](info)
        val currStr = infoMap.get(config.FORMATTED_MONTH)
        if (currentDateStr.equals(currStr)) {
          nonACBPCourseQuotaCount = infoMap.get(config.CLAIMED_NON_ACBP_COURSE_KARMA_QUOTA).asInstanceOf[Int]
        }
      }
    }
    nonACBPCourseQuotaCount = nonACBPCourseQuotaCount + nonACBPQuota
    if(nonACBPCourseQuotaCount < 0)
      nonACBPCourseQuotaCount = 0
    infoMap.put(config.CLAIMED_NON_ACBP_COURSE_KARMA_QUOTA, nonACBPCourseQuotaCount)
    infoMap.put(config.FORMATTED_MONTH, currentDateStr)
    var info = config.EMPTY
    try {
      info = mapper.writeValueAsString(infoMap)
    } catch {
      case e: JsonProcessingException =>
        throw new RuntimeException(e)
    }
    updateUserKarmaPointsSummary(userId, totalPoints + points, info)( config, cassandraUtil)
  }
}