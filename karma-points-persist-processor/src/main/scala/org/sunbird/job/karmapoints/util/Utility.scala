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
      logger.info(s"Karma points successfully updated for user $userId, contextId $contextId, points: $points")
      insertKarmaCreditLookup(userId, contextType, operationType, contextId, creditDate)(config, cassandraUtil)
      metrics.incCounter(config.dbUpdateCount)
    } else {
      val msg = s"Database update has failed for userId: $userId, contextType: $contextType, operationType: $operationType, contextId: $contextId, Points: $points"
      logger.error(msg)
      throw new Exception(msg)
    }
  }

  def doesEntryExist(userId: String, contextType: String, operationType: String, contextId: String)(implicit metrics: Metrics, config: KarmaPointsProcessorConfig, cassandraUtil: CassandraUtil): Boolean = {
    logger.info(s"Checking if entry exists for user $userId, contextType $contextType, operationType $operationType, contextId $contextId")
    val karmaPointsLookUp = fetchUserKarmaPointsCreditLookup(userId, contextType, operationType, contextId)(config, cassandraUtil)

    if (karmaPointsLookUp.size() < 1) {
      logger.info(s"No karma points lookup found for user $userId, contextType $contextType, operationType $operationType, contextId $contextId")
      return false
    }
    val creditDate = karmaPointsLookUp.get(0).getObject(config.DB_COLUMN_CREDIT_DATE).asInstanceOf[Date]
    logger.info(s"Credit date retrieved for user $userId, contextType $contextType, operationType $operationType, contextId $contextId: $creditDate")
    val result = fetchUserKarmaPoints(creditDate, userId, contextType, operationType, contextId)( config, cassandraUtil)
    if(result.size() > 0){
      logger.info(s"Karma points found for user $userId, contextType $contextType, operationType $operationType, contextId $contextId")
    }else{
      logger.info(s"No karma points found for user $userId, contextType $contextType, operationType $operationType, contextId $contextId")
    }
    result.size() > 0
  }
  def fetchUserKarmaPoints( creditDate: Date,
                            userId: String,
                            contextType: String,
                            operationType: String,
                            contextId: String
                          )(implicit config: KarmaPointsProcessorConfig, cassandraUtil: CassandraUtil): util.List[Row] = {
    logger.info(s"Fetching Karma Points for user $userId, contextType $contextType, operationType $operationType, contextId $contextId, creditDate $creditDate")
    val karmaQuery: Select = QueryBuilder
      .select()
      .from(config.sunbird_keyspace, config.user_karma_points_table)
    logger.info(s"Executing Karma Points select query: ${karmaQuery.toString}")
    karmaQuery.where(
        QueryBuilder.eq(config.DB_COLUMN_USERID, userId))
      .and(QueryBuilder.eq(config.DB_COLUMN_CREDIT_DATE, creditDate))
      .and(QueryBuilder.eq(config.DB_COLUMN_CONTEXT_TYPE, contextType))
      .and(QueryBuilder.eq(config.DB_COLUMN_OPERATION_TYPE, operationType))
      .and(QueryBuilder.eq(config.DB_COLUMN_CONTEXT_ID, contextId))
    val karmaPointsResult = cassandraUtil.find(karmaQuery.toString)
    logger.info(s"Karma Points result for user $userId, contextType $contextType, operationType $operationType, contextId $contextId, creditDate $creditDate: ${karmaPointsResult.size()} rows")
    karmaPointsResult
  }

  def isUserFirstEnrollment(userId: String)(implicit config: KarmaPointsProcessorConfig, cassandraUtil: CassandraUtil): Boolean = {
    logger.info(s"Checking if user $userId is on their first enrollment")
    val enrollmentLookupQuery: Select = QueryBuilder.select()
      .from(config.sunbird_courses_keyspace, config.user_enrollments_lookup_table)
    enrollmentLookupQuery.where(QueryBuilder.eq(config.DB_COLUMN_USERID, userId))
    logger.info(s"Executing Enrollment Lookup select query: ${enrollmentLookupQuery.toString}")
    val enrollmentLookupResult = cassandraUtil.find(enrollmentLookupQuery.toString)
    val isFirstEnrollment = enrollmentLookupResult.size() < 2

    if (isFirstEnrollment) {
      logger.info(s"User $userId is on their first enrollment")
    } else {
      logger.info(s"User $userId has more than one enrollment")
    }
    isFirstEnrollment
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
    logger.info(s"Fetching Karma Points Credit Lookup for user $userId, contextType $contextType, operationType $operationType, contextId $contextId")
    val karmaPointsLookupQuery: Select = QueryBuilder
      .select()
      .from(config.sunbird_keyspace, config.user_karma_points_credit_lookup_table)
       karmaPointsLookupQuery.where(QueryBuilder.eq(config.DB_COLUMN_USER_KARMA_POINTS_KEY, userId + config.PIPE + contextType + config.PIPE + contextId)
      ).and(QueryBuilder.eq(config.DB_COLUMN_OPERATION_TYPE, operationType))
    logger.info(s"Executing Karma Points Credit Lookup select query: ${karmaPointsLookupQuery.toString}")
    val karmaPointsLookupResult = cassandraUtil.find(karmaPointsLookupQuery.toString)
    logger.info(s"Karma Points Credit Lookup result for user $userId, contextType $contextType, operationType $operationType, contextId $contextId: ${karmaPointsLookupResult.size()} rows")
    karmaPointsLookupResult
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

  def doesAssessmentExistInHierarchy(hierarchy: java.util.Map[String, AnyRef])(implicit metrics: Metrics, config: KarmaPointsProcessorConfig): String = {
    val childrenMap = hierarchy.get(config.CHILDREN).asInstanceOf[util.ArrayList[util.HashMap[String, AnyRef]]]
    for (children <- childrenMap) {
      val primaryCategory = children.get(config.PRIMARY_CATEGORY)
      if (primaryCategory == config.COURSE_ASSESSMENT) {
         return children.get(config.IDENTIFIER).asInstanceOf[String]
      }
    }
    config.EMPTY
  }
  def fetchContentHierarchy(courseId: String)(implicit metrics: Metrics,config: KarmaPointsProcessorConfig, cassandraUtil: CassandraUtil): util.HashMap[String, AnyRef] = {
    val selectWhere: Select.Where = QueryBuilder
      .select(config.HIERARCHY)
      .from(config.content_hierarchy_KeySpace, config.content_hierarchy_table)
      .where(QueryBuilder.eq(config.IDENTIFIER, courseId))
    logger.info(s"Executing Content Hierarchy select query: ${selectWhere.toString}")
    metrics.incCounter(config.dbReadCount)
    val courseList = cassandraUtil.find(selectWhere.toString)
    if (courseList != null && courseList.size() > 0) {
      val hierarchy = courseList.get(0).getString(config.HIERARCHY)
      logger.info(s"Content Hierarchy: $hierarchy successfully retrieved for courseId: $courseId")
      mapper.readValue(hierarchy, classOf[java.util.Map[String, AnyRef]]).asInstanceOf[util.HashMap[String, AnyRef]]
    } else {
      logger.info(s"No Content Hierarchy found for courseId: $courseId")
      new util.HashMap[String, AnyRef]()
    }
  }

  def updateKarmaSummary(userId:String,points:Int)(implicit config:KarmaPointsProcessorConfig,cassandraUtil: CassandraUtil): Unit = {
    logger.info(s"Updating Karma Summary for user $userId with points: $points")
    var total_points:Int = 0
    val userKarmaSummary = fetchUserKpSummary(userId)(config, cassandraUtil)
    if(userKarmaSummary.size() > 0) {
      total_points = userKarmaSummary.get(0).getInt(config.TOTAL_POINTS)
      logger.info(s"Existing Karma Summary found for user $userId. Total points: $total_points")
    }else {
      logger.info(s"No existing Karma Summary found for user $userId")
    }

    updateUserKarmaPointsSummary(userId: String, total_points + points,null)(config: KarmaPointsProcessorConfig, cassandraUtil: CassandraUtil)
    logger.info(s"Karma Summary updated successfully for user $userId. New total points: ${total_points + points}")
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
    logger.info(s"Updating Karma Points Summary for user $userId with total points: $points")
    val query: Insert = QueryBuilder
      .insertInto(config.sunbird_keyspace, config.user_karma_summary_table)
      .value(config.USER_ID, userId)
      .value(config.TOTAL_POINTS, points)
    if (addInfo != null) {
      logger.info(s"Adding additional information to Karma Points Summary for user $userId: $addInfo")
      query.value(config.ADD_INFO, addInfo)
    }
    cassandraUtil.upsert(query.toString)
    logger.info(s"Karma Points Summary updated successfully for user $userId. New total points: $points")
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
    logger.info(s"Executing Karma Credit Lookup insert query: ${karmaCreditLookupQuery.toString}")
    val result = cassandraUtil.upsert(karmaCreditLookupQuery.toString)
    if (result) {
      logger.info(s"Karma Credit Lookup inserted successfully for user $userId, contextType $contextType, contextId $contextId, operationType $operationType")
    } else {
      logger.error(s"Failed to insert Karma Credit Lookup for user $userId, contextType $contextType, contextId $contextId, operationType $operationType")
    }
    result
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
   def fetchUserAssessmentResult(userId: String,assessmentId : String)(implicit config: KarmaPointsProcessorConfig, cassandraUtil: CassandraUtil): util.List[Row] = {
    val query: Select = QueryBuilder.select(config.DB_COLUMN_SUBMIT_ASSESSMENT_RESPONSE).from(config.sunbird_keyspace, config.user_assessment_data_table)
    query.where(QueryBuilder.eq(config.DB_COLUMN_USERID, userId)).and(QueryBuilder.eq(config.DB_COLUMN_ASSESSMENT_ID, assessmentId))
      .limit(1)
    cassandraUtil.find(query.toString)
  }
}