package org.sunbird.job.karmapoints.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.sunbird.job.BaseJobConfig

class KarmaPointsProcessorConfig(override val config: Config) extends BaseJobConfig(config, "program-karma-points-persist-processor") {

  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  val ratingOperationType:String ="RATING"
  val firstEnrolmentOperationType:String ="FIRST_ENROLMENT"
  val firstLoginOperationType:String ="FIRST_LOGIN"
  val courseCompletionOperationType:String ="COURSE_COMPLETION"

  //kafka config
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  val karmaPointsPersistProcessorConsumer: String = "karma-points-persist-processor-consumer"
  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")

  //Tags

  //Cassandra config
  val dbHost: String = config.getString("lms-cassandra.host")
  val dbPort: Int = config.getInt("lms-cassandra.port")

  val Hierarchy: String = "hierarchy"


 val sunbird_keyspace: String = config.getString("cassandra.sunbird.keyspace")
  val content_hierarchy_table: String = config.getString("cassandra.content_hierarchy.table")
  val content_hierarchy_KeySpace: String = config.getString("cassandra.content_hierarchy.keyspace")
 val user_karma_points_table: String = config.getString("cassandra.user_karma_points.table")
 val user_karma_points_credit_lookup_table: String = config.getString("cassandra.user_karma_points_credit_lookup.table")

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

}
