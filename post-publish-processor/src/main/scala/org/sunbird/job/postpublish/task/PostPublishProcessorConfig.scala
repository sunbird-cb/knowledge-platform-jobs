package org.sunbird.job.postpublish.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.job.BaseJobConfig
import org.sunbird.job.postpublish.functions.PublishMetadata
import scala.collection.immutable.Map
import java.util

class PostPublishProcessorConfig(override val config: Config) extends BaseJobConfig(config, "post-publish-processor") {

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
  implicit val publishMetaTypeInfo: TypeInformation[PublishMetadata] = TypeExtractor.getForClass(classOf[PublishMetadata])

  // Job Configuration
  val jobEnv: String = config.getString("job.env")

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
  val contentPublishTopic: String = config.getString("kafka.publish.topic")

  val inputConsumerName = "post-publish-event-consumer"

  // Parallelism
  val eventRouterParallelism: Int = config.getInt("task.router.parallelism")
  val shallowCopyParallelism: Int = config.getInt("task.shallow_copy.parallelism")
  val linkDialCodeParallelism: Int = config.getInt("task.link_dialcode.parallelism")
  val batchCreateParallelism: Int = config.getInt("task.batch_create.parallelism")
  val postPublishRelationUpdateParallelism: Int = config.getInt("task.post-publish-relation-update.parallelism")

  // Metric List
  val totalEventsCount = "total-events-count"
  val skippedEventCount = "skipped-events-count"
  val batchCreationCount = "batch-creation-count"
  val batchCreationSuccessCount = "batch-creation-success-count"
  val batchCreationFailedCount = "batch-creation-failed-count"
  val shallowCopyCount = "shallow-copy-count"
  val dialLinkCount = "dial-link-count"
  val dialLinkSuccessCount = "dial-link-success-count"
  val dialLinkFailedCount = "dial-link-failed-count"
  val qrImageGeneratorEventCount = "qr-image-event-count"
  val postPublishRelationUpdateEventCount = "post-publish-relation-update-count"
  val postPublishRelationUpdateSuccessCount = "post-publish-relation-update-success-count"
  val postPublishRelationUpdateFailureCount = "post-publish-relation-update-failure-count"

  // Cassandra Configurations
  val dbHost: String = config.getString("lms-cassandra.host")
  val dbPort: Int = config.getInt("lms-cassandra.port")
  val lmsKeyspaceName = config.getString("lms-cassandra.keyspace")
  val sunbirdKeyspaceName = config.getString("lms-cassandra.sbKeyspace")
  val defaultCertTemplateId = config.getString("lms-cassandra.certTemplateId")
  val sbSystemSettingsTableName = config.getString("lms-cassandra.systemSettingsTable")
  val batchTableName = config.getString("lms-cassandra.batchTable")
  val hierarchyStoreKeySpace = config.getString("lms-cassandra.hierarchyStoreKeySpace")
  val dialcodeKeyspaceName = config.getString("dialcode-cassandra.keyspace")
  val dialcodeTableName = config.getString("dialcode-cassandra.imageTable")

  // Neo4J Configurations
  val graphRoutePath = config.getString("neo4j.routePath")
  val graphName = config.getString("neo4j.graph")


  // Tags
  val batchCreateOutTag: OutputTag[java.util.Map[String, AnyRef]] = OutputTag[java.util.Map[String, AnyRef]]("batch-create")
  val linkDIALCodeOutTag: OutputTag[java.util.Map[String, AnyRef]] = OutputTag[java.util.Map[String, AnyRef]]("dialcode-link")
  val shallowContentPublishOutTag: OutputTag[PublishMetadata] = OutputTag[PublishMetadata]("shallow-copied-content-publish")
  val publishEventOutTag: OutputTag[String] = OutputTag[String]("content-publish-request")
  val generateQRImageOutTag: OutputTag[String] = OutputTag[String]("qr-image-generator-request")
  val postPublishRelationUpdateOutTag:OutputTag[String]= OutputTag[String]("post-publish-relation-update")

  val searchBaseUrl = config.getString("service.search.basePath")
  val lmsBaseUrl = config.getString("service.lms.basePath")
  val learningBaseUrl = config.getString("service.learning_service.basePath")
  val dialBaseUrl = config.getString("service.dial.basePath")

  // API URLs
  val batchCreateAPIPath = lmsBaseUrl + "/private/v1/course/batch/create"
  val searchAPIPath = searchBaseUrl + "/v3/search"
  val reserveDialCodeAPIPath = learningBaseUrl + "/content/v3/dialcode/reserve"
  val batchAddCertTemplateAPIPath = lmsBaseUrl + "/private/v1/course/batch/cert/template/add"


  // QR Image Generator
  val QRImageGeneratorTopic: String = config.getString("kafka.qrimage.topic")
  val primaryCategories: util.List[String] = if (config.hasPath("dialcode.linkable.primaryCategory")) config.getStringList("dialcode.linkable.primaryCategory") else util.Arrays.asList("Course") //List[String]("Course")

  val contentServiceBase: String = config.getString("content.basePath")
  val contentReadURL = contentServiceBase+ "/content/v3/read/"

  val contentHierarchyTable: String = "content_hierarchy"
  val identifier: String = "identifier"
  val Hierarchy: String = "hierarchy"
  val children: String = "children"
  val primaryCategory: String = "primaryCategory"
  val versionKey: String = "versionKey"
  val course: String = "Course"
  val parentCollections: String="parentCollections"

  val contentSystemUpdatePath = learningBaseUrl + "/system/v3/content/update/"
  val defaultHeaders = Map[String, String] ("Content-Type" -> "application/json")
  val userAccBlockedErrCode = "UOS_USRRED0006"
  val name: String = "name"

  val contentCacheStore: Int = 0


}
