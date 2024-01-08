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
import scala.collection.immutable.Map

object Utility {
  private val logger = LoggerFactory.getLogger("org.sunbird.job.karmapoints.util.Utility")
  lazy private val mapper: ObjectMapper = new ObjectMapper()

   def insertKarmaPoints(userId : String, contextType : String,operationType:String,contextId:String, points:Int,config: KarmaPointsProcessorConfig,cassandraUtil: CassandraUtil)(implicit metrics: Metrics): Unit = {
    insertKarmaPoints(userId, contextType,operationType,contextId, points,"",config, cassandraUtil)(metrics)
  }

  def upsertKarmaPoints(userId : String, contextType : String,operationType:String,contextId:String,points:Int,
                        addInfo:String,credit_date : Long,config: KarmaPointsProcessorConfig,cassandraUtil: CassandraUtil): Boolean = {
    val query: Insert = QueryBuilder.insertInto(config.sunbird_keyspace, config.user_karma_points_table)
    query.value(config.USER_ID, userId)
    query.value(config.CREDIT_DATE,  credit_date)
    query.value(config.CONTEXT_TYPE, contextType)
    query.value(config.OPERATION_TYPE, operationType)
    query.value(config.CONTEXT_ID, contextId)
    query.value(config.ADD_INFO, addInfo)
    query.value(config.POINTS,points)
    cassandraUtil.upsert(query.toString)
  }
   def insertKarmaPoints( userId : String, contextType : String,operationType:String,contextId:String,points:Int,
                          addInfo:String,config: KarmaPointsProcessorConfig,cassandraUtil: CassandraUtil)(implicit metrics: Metrics): Unit = {
     val credit_date = System.currentTimeMillis()
     val result = upsertKarmaPoints(userId, contextType, operationType, contextId, points, addInfo, credit_date,config, cassandraUtil)
    if (result) {
      insertKarmaCreditLookUp(userId, contextType,operationType,contextId,credit_date,config, cassandraUtil)
      metrics.incCounter(config.dbUpdateCount)
    } else {
      val msg = "Database update has failed for userId :- "+userId +" ,contextType : "+
        contextType +",operationType : " + operationType+", contextId :"+ contextId +", Points" + points
      logger.error(msg)
      throw new Exception(msg)
    }
  }



  def getUserKarmaSummary(userId: String,config: KarmaPointsProcessorConfig,cassandraUtil: CassandraUtil) :util.List[Row] = {
    val karma_query: Select = QueryBuilder.select().from(config.sunbird_keyspace, config.user_karma_summary_table)
    karma_query.where(QueryBuilder.eq(config.DB_COLUMN_USERID, userId))
    cassandraUtil.find(karma_query.toString)
  }

  def updateUserKarmaSummary(userId:String,points:Int, addinfo:String,config: KarmaPointsProcessorConfig,cassandraUtil: CassandraUtil) : Unit = {
    val query: Insert = QueryBuilder.insertInto(config.sunbird_keyspace, config.user_karma_summary_table)
    query.value(config.USER_ID, userId)
    query.value(config.TOTAL_POINTS,  points)
    if(addinfo != null)
    query.value(config.ADD_INFO, addinfo)
    cassandraUtil.upsert(query.toString)
 }

   def isEntryAlreadyExist(userId: String, contextType: String, operationType: String, contextId: String,config: KarmaPointsProcessorConfig,cassandraUtil: CassandraUtil): Boolean = {
     val karmaPointsLookUp = karmaPointslookup(userId, contextType, operationType, contextId,config, cassandraUtil)
     if(karmaPointsLookUp.size() < 1)
       return false
     val credit_date = karmaPointsLookUp.get(0).getObject(config.DB_COLUMN_CREDIT_DATE).asInstanceOf[Date]
     val result_ = karmaPointsEntry(credit_date,userId, contextType, operationType, contextId,config, cassandraUtil)
    result_.size() > 0
  }


  def karmaPointsEntry(credit_date:Date ,userId: String, contextType: String, operationType: String, contextId: String
                          ,config: KarmaPointsProcessorConfig,cassandraUtil: CassandraUtil):  util.List[Row] = {
    val karma_query: Select = QueryBuilder.select().from(config.sunbird_keyspace, config.user_karma_points_table)
    karma_query.where(QueryBuilder.eq(config.DB_COLUMN_USERID, userId))
    karma_query.where(QueryBuilder.eq(config.DB_COLUMN_CREDIT_DATE, credit_date))
    karma_query.where(QueryBuilder.eq(config.DB_COLUMN_CONTEXT_TYPE, contextType))
    karma_query.where(QueryBuilder.eq(config.DB_COLUMN_OPERATION_TYPE, operationType))
    karma_query.where(QueryBuilder.eq(config.DB_COLUMN_CONTEXT_ID, contextId))
    cassandraUtil.find(karma_query.toString)
  }

  def fetchKarmaPointsByCreditDateRange(credit_date:Long ,userId: String, contextType: String, operationType: String
                       ,config: KarmaPointsProcessorConfig,cassandraUtil: CassandraUtil):  util.List[Row] = {
    val karma_query: Select = QueryBuilder.select().from(config.sunbird_keyspace, config.user_karma_points_table)
    karma_query.where(QueryBuilder.eq(config.DB_COLUMN_USERID, userId))
    karma_query.where(QueryBuilder.gt(config.DB_COLUMN_CREDIT_DATE, credit_date))
     //karma_query.where(QueryBuilder.eq(config.DB_COLUMN_CONTEXT_TYPE, contextType))
    //karma_query.where(QueryBuilder.eq(config.DB_COLUMN_OPERATION_TYPE, operationType))
    cassandraUtil.find(karma_query.toString)
  }

  def isFirstEnrolment( userid: String, config: KarmaPointsProcessorConfig, cassandraUtil: CassandraUtil): Boolean = {
    val enrol_lookup_query: Select = QueryBuilder.select().from(config.sunbird_courses_keyspace, config.user_enrolments_lookup_table)
    enrol_lookup_query.where(QueryBuilder.eq(config.DB_COLUMN_USERID, userid))
    cassandraUtil.find(enrol_lookup_query.toString).size() < 2
  }

  def userRootOrgId(userid: String, config: KarmaPointsProcessorConfig, cassandraUtil: CassandraUtil): String = {
    val enrol_lookup_query: Select = QueryBuilder.select().from(config.sunbird_keyspace, config.user_table)
    val columnNames: util.List[String] = new util.ArrayList[String]()
    columnNames.add("rootorgid")
    enrol_lookup_query.where(QueryBuilder.eq(config.ID, userid))
    cassandraUtil.find(enrol_lookup_query.toString).get(0).getString("rootorgid")
  }

  def karmaPointslookup(userId: String, contextType: String, operationType: String, contextId: String,
                        config: KarmaPointsProcessorConfig,cassandraUtil: CassandraUtil): util.List[Row] = {
    val query: Select = QueryBuilder.select().from(config.sunbird_keyspace, config.user_karma_points_credit_lookup_table)
    query.where(QueryBuilder.eq(config.DB_COLUMN_USER_KARMA_POINTS_KEY, userId + config.PIPE + contextType + config.PIPE  + contextId))
    query.where(QueryBuilder.eq(config.DB_COLUMN_OPERATION_TYPE, operationType))
    cassandraUtil.find(query.toString)
  }

   def insertKarmaCreditLookUp(userId : String, contextType : String,operationType:String,contextId:String, credit_date: Long,config: KarmaPointsProcessorConfig,cassandraUtil: CassandraUtil): Boolean = {
    val query: Insert = QueryBuilder.insertInto(config.sunbird_keyspace, config.user_karma_points_credit_lookup_table)
    query.value(config.DB_COLUMN_USER_KARMA_POINTS_KEY, userId + config.PIPE+contextType+config.PIPE+ contextId)
    query.value(config.DB_COLUMN_OPERATION_TYPE, operationType)
    query.value(config.DB_COLUMN_CREDIT_DATE, credit_date)
    cassandraUtil.upsert(query.toString)
  }



   def isAssessmentExist(hierarchy:java.util.Map[String, AnyRef],config: KarmaPointsProcessorConfig)(implicit metrics: Metrics):Boolean = {
    var result:Boolean = false;
    val childrenMap = hierarchy.get(config.CHILDREN).asInstanceOf[util.ArrayList[util.HashMap[String, AnyRef]]];
    for (children <- childrenMap) {
      val primaryCategory = children.get(config.primaryCategory)
      if (primaryCategory == config.COURSE_ASSESSMENT) result = true
    }
    result;
  }

   def isACBP(courseId: String, httpUtil: HttpUtil,config: KarmaPointsProcessorConfig,headers : Map[String, String])(implicit metrics: Metrics):Boolean = {
    isCbpPlan(courseId,headers)(metrics, config, httpUtil)
  }
   def fetchContentHierarchy(courseId: String,config: KarmaPointsProcessorConfig,cassandraUtil: CassandraUtil)(implicit metrics: Metrics): util.HashMap[String,AnyRef] = {
    val selectWhere: Select.Where = QueryBuilder.select(config.Hierarchy)
      .from(config.content_hierarchy_KeySpace, config.content_hierarchy_table).where()
    selectWhere.and(QueryBuilder.eq(config.identifier, courseId))
    metrics.incCounter(config.dbReadCount)
    val courseList = cassandraUtil.find(selectWhere.toString)
    if (null != courseList && courseList.size() > 0 ) {
      val hierarchy = courseList.get(0).getString("hierarchy")
      mapper.readValue(hierarchy, classOf[java.util.Map[String, AnyRef]]).asInstanceOf[util.HashMap[String, AnyRef]]
    }else
    {
      new util.HashMap[String, AnyRef]()
    }
  }

  def isCbpPlan(courseId: String,headers : Map[String, String])(
    metrics: Metrics,
    config: KarmaPointsProcessorConfig,
    httpUtil: HttpUtil
  ): Boolean= {
    val url =
      config.cbPlanReadUser
    val response = getAPICall(url,headers)(config, httpUtil, metrics)
    val identifiers: List[AnyRef] = response.get(config.CONTENT) match {
      case Some(content) =>
        content.asInstanceOf[List[Map[String, AnyRef]]].flatMap { contentItem =>
          contentItem.get(config.CONTENT_LIST) match {
            case Some(contentList) =>
              contentList.asInstanceOf[List[Map[String, AnyRef]]].flatMap(_.get(config.IDENTIFIER))
            case None =>
              List.empty[AnyRef] // or handle the case when "contentList" is not present in the response
          }
        }
      case None =>
        List.empty[AnyRef] // or handle the case when "content" is not present in the response
    }
    identifiers.contains(courseId)
  }

  def getAPICall(url: String, headers : Map[String, String])(
    config: KarmaPointsProcessorConfig,
    httpUtil: HttpUtil,
    metrics: Metrics
  ): Map[String, AnyRef] = {
    val response = httpUtil.get(url, headers)
    if (200 == response.status) {
      ScalaJsonUtil
        .deserialize[Map[String, AnyRef]](response.body)
        .getOrElse(config.RESULT, Map[String, AnyRef]())
        .asInstanceOf[Map[String, AnyRef]]
    } else if (
      400 == response.status && response.body.contains(
        config.userAccBlockedErrCode
      )
    ) {
      metrics.incCounter(config.skippedEventCount)
      logger.error(
        s"Error while fetching user details for ${url}: " + response.status + " :: " + response.body
      )
      Map[String, AnyRef]()
    } else {
      throw new Exception(
        s"Error from get API : ${url}, with response: ${response}"
      )
    }
  }

   def courseCompletion(userId : String, contextType : String,operationType:String,
                               contextId:String,hierarchy:java.util.Map[String, AnyRef],
                               config: KarmaPointsProcessorConfig,
                               httpUtil: HttpUtil,cassandraUtil: CassandraUtil)(metrics: Metrics) :Unit = {
    var points : Int = config.courseCompletionQuotaKarmaPoints
    val addInfoMap = new util.HashMap[String, AnyRef]
     addInfoMap.put(config.ADDINFO_ASSESSMENT, java.lang.Boolean.FALSE)
     addInfoMap.put(config.ADDINFO_ACBP, java.lang.Boolean.FALSE)
     addInfoMap.put(config.OPERATION_COURSE_COMPLETION, java.lang.Boolean.TRUE)
     addInfoMap.put(config.ADDINFO_COURSENAME, hierarchy.get(config.name))
    if(Utility.isAssessmentExist(hierarchy,config)(metrics)) {
      points = points+config.assessmentQuotaKarmaPoints
      addInfoMap.put(config.ADDINFO_ASSESSMENT, java.lang.Boolean.TRUE)
    }
     val headers = Map[String, String](
       config.HEADER_CONTENT_TYPE_KEY -> config.HEADER_CONTENT_TYPE_JSON
       , config.X_AUTHENTICATED_USER_ORGID-> Utility.userRootOrgId(userId, config, cassandraUtil)
       , config.X_AUTHENTICATED_USER_ID -> userId)

     val isACBP = Utility.isACBP(contextId, httpUtil, config, headers)(metrics)
    if(isACBP){
      points = points+config.acbpQuotaKarmaPoints
      addInfoMap.put(config.ADDINFO_ACBP, java.lang.Boolean.TRUE)
    }
    var addInfo = config.EMPTY
    try addInfo = mapper.writeValueAsString(addInfoMap)
    catch {
      case e: JsonProcessingException =>
        throw new RuntimeException(e)
    }
       Utility.insertKarmaPoints(userId, contextType,operationType,contextId,points, addInfo,config, cassandraUtil)(metrics)
       updateKarmaSummaryForCourseCompletion(userId, points, isACBP,config, cassandraUtil)
   }

  def updateKarmaSummaryForCourseCompletion (userId:String,points:Int,isACBP:Boolean,config:KarmaPointsProcessorConfig,cassandraUtil: CassandraUtil): Unit = {
    var total_points:Int = 0
    var infoMap = new util.HashMap[String, Any]
    var nonACBPCourseQuotaCount : Int= 0
    val currentDate = LocalDate.now
    val formatter = DateTimeFormatter.ofPattern(config.YYYY_PIPE_MM)
    val currentDateStr = currentDate.format(formatter)
    val userKarmaSummary = Utility.getUserKarmaSummary(userId, config, cassandraUtil)
    if(userKarmaSummary.size() > 0) {
      total_points = userKarmaSummary.get(0).getInt(config.TOTAL_POINTS)
      val info = userKarmaSummary.get(0).getString(config.ADD_INFO)
      if (!StringUtils.isEmpty(info)) {
      infoMap = JSONUtil.deserialize[java.util.HashMap[String, Any]](info)
      val currStr = infoMap.get(config.FORMATTED_MONTH)
      if(currentDateStr.equals(currStr)){
        nonACBPCourseQuotaCount = infoMap.get(config.CLAIMED_NON_ACBP_COURSE_KARMA_QUOTA).asInstanceOf[Int]
      } }
    }
    if (!isACBP) {
      nonACBPCourseQuotaCount = nonACBPCourseQuotaCount+1
    }
    infoMap.put(config.CLAIMED_NON_ACBP_COURSE_KARMA_QUOTA, nonACBPCourseQuotaCount)
    infoMap.put(config.FORMATTED_MONTH,currentDateStr)
    var info = config.EMPTY
    try info = mapper.writeValueAsString(infoMap)
    catch {
      case e: JsonProcessingException =>
        throw new RuntimeException(e)
    }
    Utility.updateUserKarmaSummary(userId: String, total_points + points, info, config: KarmaPointsProcessorConfig, cassandraUtil: CassandraUtil)
  }

  def updateKarmaSummary(userId:String,points:Int,config:KarmaPointsProcessorConfig,cassandraUtil: CassandraUtil): Unit = {
    var total_points:Int = 0
    val userKarmaSummary = Utility.getUserKarmaSummary(userId, config, cassandraUtil)
    if(userKarmaSummary.size() > 0) {
      total_points = userKarmaSummary.get(0).getInt(config.TOTAL_POINTS)
    }
    Utility.updateUserKarmaSummary(userId: String, total_points + points,null, config: KarmaPointsProcessorConfig, cassandraUtil: CassandraUtil)
  }

  def updateKarmaSummaryForClaimACBP (userId:String,points:Int,config:KarmaPointsProcessorConfig,cassandraUtil: CassandraUtil): Unit = {
    var total_points:Int = 0
    var infoMap = new util.HashMap[String, Any]
    var nonACBPCourseQuotaCount : Int= 0
    val currentDate = LocalDate.now
    val formatter = DateTimeFormatter.ofPattern(config.YYYY_PIPE_MM)
    val currentDateStr = currentDate.format(formatter)
    val userKarmaSummary = Utility.getUserKarmaSummary(userId, config, cassandraUtil)
    if(userKarmaSummary.size() > 0) {
      total_points = userKarmaSummary.get(0).getInt(config.TOTAL_POINTS)
      val info = userKarmaSummary.get(0).getString(config.ADD_INFO)
      if (!StringUtils.isEmpty(info)) {
        infoMap = JSONUtil.deserialize[java.util.HashMap[String, Any]](info)
        val currStr = infoMap.get(config.FORMATTED_MONTH)
        if (currentDateStr.equals(currStr)) {
          nonACBPCourseQuotaCount = infoMap.get(config.CLAIMED_NON_ACBP_COURSE_KARMA_QUOTA).asInstanceOf[Int]
        }
      }
    }
    if (nonACBPCourseQuotaCount > 0) {
      nonACBPCourseQuotaCount = nonACBPCourseQuotaCount-1
    }
    infoMap.put(config.CLAIMED_NON_ACBP_COURSE_KARMA_QUOTA, nonACBPCourseQuotaCount)
    infoMap.put(config.FORMATTED_MONTH,currentDateStr)
    var info = config.EMPTY
    try info = mapper.writeValueAsString(infoMap)
    catch {
      case e: JsonProcessingException =>
        throw new RuntimeException(e)
    }
    Utility.updateUserKarmaSummary(userId: String, total_points + points, info, config: KarmaPointsProcessorConfig, cassandraUtil: CassandraUtil)
  }

}
