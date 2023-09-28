package org.sunbird.job.programcert.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.job.BaseJobConfig

import java.util

class ProgramCertPreProcessorConfig(override val config: Config) extends BaseJobConfig(config, "program-cert-pre-processor") {

    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
    
    //Redis config
    val collectionCacheStore: Int = 0
    val contentCacheStore: Int = 5
    val metaRedisHost: String = config.getString("redis-meta.host")
    val metaRedisPort: Int = config.getInt("redis-meta.port")
    
    //kafka config
    val kafkaInputTopic: String = config.getString("kafka.input.topic")
    val kafkaOutputTopic: String = config.getString("kafka.output.topic")
    val certificatePreProcessorConsumer: String = "program-cert-pre-processor-consumer"
    val generateCertificateProducer = "generate-certificate-sink"
    override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
    val generateCertificateParallelism:Int = config.getInt("task.generate_certificate.parallelism")
    
    //Tags
    val generateCertificateOutputTagName = "generate-certificate-request"
    val generateCertificateOutputTag: OutputTag[String] = OutputTag[String](generateCertificateOutputTagName)

    //Cassandra config
    val dbHost: String = config.getString("lms-cassandra.host")
    val dbPort: Int = config.getInt("lms-cassandra.port")
    val keyspace: String = config.getString("lms-cassandra.keyspace")
    val userEnrolmentsTable: String = config.getString("lms-cassandra.user_enrolments.table")
    val dbBatchId = "batchId"
    val dbCourseId = "courseid"
    val dbUserId = "userid"
    val contentHierarchyTable: String = "content_hierarchy"
    val contentHierarchyKeySpace: String = "dev_hierarchy_store"
    val Hierarchy: String = "hierarchy"
    val childrens: String = "children"
    val batches: String = "batches"
    
    //API URL
    val contentBasePath = config.getString("service.content.basePath")
    val learnerBasePath = config.getString("service.learner.basePath")
    val userReadApi = config.getString("user_read_api")
    val contentReadApi = "/content/v4/read"

    // Metric List
    val totalEventsCount = "total-events-count"
    val successEventCount = "success-events-count"
    val failedEventCount = "failed-events-count"
    val skippedEventCount = "skipped-event-count"
    val dbReadCount = "db-read-count"
    val dbUpdateCount = "db-update-count"
    val cacheHitCount = "cache-hit-cout"
    
    //Constants
    val status: String = "status"
    val name: String = "name"
    val defaultHeaders = Map[String, String] ("Content-Type" -> "application/json")
    val identifier: String = "identifier"
    val userAccBlockedErrCode = "UOS_USRRED0006"
    val programCertPreProcess: String = "program_cert_pre_process"
    val parentCollections: String = "parentCollections"

}
