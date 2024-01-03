package org.sunbird.job.karmapoints.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.sunbird.job.BaseJobConfig

class KarmaPointsProcessorConfig(override val config: Config) extends BaseJobConfig(config, "program-karma-points-persist-processor") {

  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  //kafka config
  val kafkaInputCourseCompletionTopic: String = config.getString("kafka.input.course.completion.topic")
  val kafkaInputRatingTopic: String = config.getString("kafka.input.rating.topic")
  val kafkaInputFirstLoginTopic: String = config.getString("kafka.input.first.login.topic")
  val kafkaInputFirstEnrolmentTopic: String = config.getString("kafka.input.first.enrolment.topic")
  val kafkaInputClaimACBPTopic: String = config.getString("kafka.input.claim.acbp.karma.points.topic")

  val karmaPointsRatingPersistProcessorConsumer: String = "karma-points-rating-persist-consumer"
  val karmaPointsCourseCompletionPersistProcessorConsumer: String = "karma-points-course-completion-persist-consumer"
  val karmaPointsFirstLoginPersistProcessorConsumer: String = "karma-points-first-login-persist-processor-consumer"
  val karmaPointsFirstEnrolmentPersistProcessorConsumer: String = "karma-points-first-enrolment-persist-processor-consumer"
  val karmaPointsClaimACBPPersistProcessorConsumer: String = "karma-points-acbp-claim-karma-points-persist-processor-consumer"

  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")

  //Tags

  //Cassandra config
  val dbHost: String = config.getString("lms-cassandra.host")
  val dbPort: Int = config.getInt("lms-cassandra.port")

  val Hierarchy: String = "hierarchy"

  val ADDINFO_ASSESSMENT="ASSESSMENT"
  val ADDINFO_ACBP="ACBP"
  val ADDINFO_COURSENAME="COURSENAME"

  val EDATA="edata"
  val USERIDS = "userIds"

  val COURSE_ID = "courseId"
  val BATCH_ID = "batchId"


  val OPERATION_COURSE_COMPLETION = "COURSE_COMPLETION"

  val sunbird_keyspace: String = config.getString("cassandra.sunbird.keyspace")
  val sunbird_courses_keyspace: String = config.getString("cassandra.sunbird_courses.keyspace")

  val content_hierarchy_table: String = config.getString("cassandra.content_hierarchy.table")
  val content_hierarchy_KeySpace: String = config.getString("cassandra.content_hierarchy.keyspace")
 val user_karma_points_table: String = config.getString("cassandra.user_karma_points.table")
 val user_karma_points_credit_lookup_table: String = config.getString("cassandra.user_karma_points_credit_lookup.table")
  val user_enrolments_lookup_table: String = config.getString("cassandra.user_enrolments.table")
  val user_table: String = config.getString("cassandra.user.table")
  // Metric List
  val totalEventsCount = "total-events-count"
  val successEventCount = "success-events-count"
  val failedEventCount = "failed-events-count"
  val skippedEventCount = "skipped-event-count"
  val dbReadCount = "db-read-count"
  val dbUpdateCount = "db-update-count"
  val cacheHitCount = "cache-hit-cout"
  val karmaPointsIssueEventsCount = "karma-points-issue-events-count"
  val cacheMissCount = "cache-miss-count"

  //Constants
  val name: String = "name"
  val identifier: String = "identifier"
  val primaryCategory: String = "primaryCategory"
  val cbPlanBase: String = config.getString("service.cbplan.basePath")

  val cbPlanReadUser = cbPlanBase + "cbplan/v1/private/user/list"

  val defaultHeaders = Map[String, String]("Content-Type" -> "application/json"
    ,"x-authenticated-user-token"->"eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJwMTRYR1lrdHAxUnNScEZZeXZGTnZuekUxVDBMT3hVSHBoNnhHSzUxdGhvIn0.eyJqdGkiOiIyNzEyZTU3NC05ODJjLTQwM2ItYmQ3ZC00MmE1MjE3OTI5ODAiLCJleHAiOjE3MDM5ODQ1MzQsIm5iZiI6MCwiaWF0IjoxNzAzOTQxMzM0LCJpc3MiOiJodHRwczovL3BvcnRhbC5rYXJtYXlvZ2kubmljLmluL2F1dGgvcmVhbG1zL3N1bmJpcmQiLCJhdWQiOiJhY2NvdW50Iiwic3ViIjoiZjpiYWFkYThiMS1lNjJlLTQ0YzQtYTE0ZC02NzAyZWE5MGY0OTY6MzI2NTk1OGEtNjYyOC00M2ViLTlmMWMtMThmZWM4ZTYwNzE0IiwidHlwIjoiQmVhcmVyIiwiYXpwIjoiYW5kcm9pZCIsImF1dGhfdGltZSI6MCwic2Vzc2lvbl9zdGF0ZSI6ImVhN2ExMDA3LWEwZWMtNDJmOC1iMWMzLTM0ODQ3ZTIzNzZjZCIsImFjciI6IjEiLCJyZWFsbV9hY2Nlc3MiOnsicm9sZXMiOlsib2ZmbGluZV9hY2Nlc3MiLCJ1bWFfYXV0aG9yaXphdGlvbiJdfSwicmVzb3VyY2VfYWNjZXNzIjp7ImFjY291bnQiOnsicm9sZXMiOlsibWFuYWdlLWFjY291bnQiLCJtYW5hZ2UtYWNjb3VudC1saW5rcyIsInZpZXctcHJvZmlsZSJdfX0sInNjb3BlIjoiIiwibmFtZSI6ImJoYXJhdGgga3VtYXIgUiIsInByZWZlcnJlZF91c2VybmFtZSI6ImJoYXJhdGhrdW1hcnJfd3lmeCIsImdpdmVuX25hbWUiOiJiaGFyYXRoIGt1bWFyIFIiLCJlbWFpbCI6ImJoKioqKioqKipAeW9wbWFpbC5jb20ifQ.cUSurme6qLbvbz0sbdtsd5A6QvYgxUgLP59YRzVuaKuKTrfEbDdRNqF58H7hEY99oiAXrK5E4TxyFnZH7JKZU1iRifNBY3J-Otj9vwSVNyoAIrFh8ODNXtOSe-_tsPh0G1zj1cgNdjtOKxiQYuO67aiJDTpxNZr3fnx6HZ-hJT9deMZd08ZlVfbSDRLFsxCmtJMgH5DY8-YtvBz4s7yWP-3LbAi9X6m3ffNVU8ctUvl2DSUphZWI3XZDdndzShadjWvdgM1c-FS8lZxZcQD9HM0_ncnAlh_aKntBKdrQD2K5x1a5SwvVoJfvI-iZxyNbvpnHoaIyyPzaO9bvsHDcpQ"
    ,"x-authenticated-user-orgid"->"01379305664500531251")

  val parentCollections: String = "parentCollections"
  val userAccBlockedErrCode = "UOS_USRRED0006"

  val courseCompletionPoints = 5
  val acbpQuotaKarmaPoints = 10
  val assessmentQuotaKarmaPoints = 5
  val ratingQuotaKarmaPoints = 5
  val firstLoginQuotaKarmaPoints = 5
  val firstEnrolmentQuotaKarmaPoints = 5


  val PRIMARY_CATEGORY ="primaryCategory"
  val USER_ID ="userid"
  val CREDIT_DATE = "credit_date"
  val CONTEXT_TYPE = "context_type"
  val OPERATION_TYPE = "operation_type"
  val CONTEXT_ID = "context_id"
  val ADD_INFO = "addinfo"
  val POINTS = "points"
  val USER_ID_CAMEL ="userId"

  val UNDER_SCORE= "_"
  val DB_COLUMN_USER_KARMA_POINTS_KEY= "user_karma_points_key"
  val DB_COLUMN_OPERATION_TYPE= "operation_type"
  val DB_COLUMN_USERID= "userid"
  val DB_COLUMN_CREDIT_DATE= "credit_date"
  val DB_COLUMN_CONTEXT_TYPE= "context_type"
  val DB_COLUMN_CONTEXT_ID= "context_id"
  val DB_COLUMN_BATCH_ID= "batchid"

  val CHILDREN = "children"
  val COURSE_ASSESSMENT="Course Assessment"
  val CONTENT = "content"
  val CONTENT_LIST = "contentList"
  val IDENTIFIER = "identifier"
  val RESULT = "result"

  val OPERATION_TYPE_RATING ="RATING"
  val OPERATION_TYPE_FIRST_LOGIN ="FIRST_LOGIN"
  val OPERATION_TYPE_ENROLMENT:String ="FIRST_ENROLMENT"
  val ACTIVITY_ID = "activity_id"
  val ID = "id"
}

