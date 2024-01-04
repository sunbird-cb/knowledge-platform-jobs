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


  val EDATA="edata"
  val USERIDS = "userIds"

  val COURSE_ID = "courseId"
  val BATCH_ID = "batchId"



  val sunbird_keyspace: String = config.getString("cassandra.sunbird.keyspace")
  val sunbird_courses_keyspace: String = config.getString("cassandra.sunbird_courses.keyspace")
  val content_hierarchy_KeySpace: String = config.getString("cassandra.content_hierarchy.keyspace")

  val content_hierarchy_table: String = config.getString("cassandra.content_hierarchy.table")
  val user_karma_points_table: String = config.getString("cassandra.user_karma_points.table")
  val user_karma_points_credit_lookup_table: String = config.getString("cassandra.user_karma_points_credit_lookup.table")
  val user_enrolments_lookup_table: String = config.getString("cassandra.user_enrolments.table")
  val user_table: String = config.getString("cassandra.user.table")
  val user_karma_summary_table: String = config.getString("cassandra.user_karma_points_summary.table")

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

  val userAccBlockedErrCode = "UOS_USRRED0006"

  val acbpQuotaKarmaPoints = config.getInt("karmapoints.acbpQuotaKarmaPoints")
  val courseCompletionQuotaKarmaPoints = config.getInt("karmapoints.courseCompletionQuotaKarmaPoints")
  val assessmentQuotaKarmaPoints = config.getInt("karmapoints.assessmentQuotaKarmaPoints")
  val ratingQuotaKarmaPoints = config.getInt("karmapoints.ratingQuotaKarmaPoints")
  val firstLoginQuotaKarmaPoints = config.getInt("karmapoints.firstLoginQuotaKarmaPoints")
  val firstEnrolmentQuotaKarmaPoints = config.getInt("karmapoints.firstEnrolmentQuotaKarmaPoints")
  val nonAcbpCourseQuota = config.getInt("karmapoints.nonAcbpCourseQuota")


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
  val PIPE= "|"

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
  val OPERATION_COURSE_COMPLETION = "COURSE_COMPLETION"

  val ADDINFO_ASSESSMENT="ASSESSMENT"
  val ADDINFO_ACBP="ACBP"
  val ADDINFO_COURSENAME="COURSENAME"

  val ACTIVITY_ID = "activity_id"
  val ID = "id"

  val HEADER_CONTENT_TYPE_KEY = "Content-Type"
  val HEADER_CONTENT_TYPE_JSON = "application/json"
  val X_AUTHENTICATED_USER_ORGID = "x-authenticated-user-orgid"
  val X_AUTHENTICATED_USER_ID = "x-authenticated-userid"

}

