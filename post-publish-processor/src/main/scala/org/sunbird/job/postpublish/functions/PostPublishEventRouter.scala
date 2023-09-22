package org.sunbird.job.postpublish.functions

import com.google.gson.reflect.TypeToken
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.cache.{DataCache, RedisConnect}
import org.sunbird.job.postpublish.domain.Event
import org.sunbird.job.postpublish.helpers.{BatchCreation, DialHelper, PostPublishRelationUpdater, ShallowCopyPublishing}
import org.sunbird.job.postpublish.task.PostPublishProcessorConfig
import org.sunbird.job.util.{CassandraUtil, HttpUtil, Neo4JUtil}
import org.sunbird.job.{BaseProcessFunction, Metrics}

import java.lang.reflect.Type

case class PublishMetadata(identifier: String, contentType: String, mimeType: String, pkgVersion: Int)

class PostPublishEventRouter(config: PostPublishProcessorConfig, httpUtil: HttpUtil,
                             @transient var neo4JUtil: Neo4JUtil = null,
                             @transient var cassandraUtil: CassandraUtil = null)
  extends BaseProcessFunction[Event, String](config) with ShallowCopyPublishing with BatchCreation with DialHelper with PostPublishRelationUpdater {

  private[this] val logger = LoggerFactory.getLogger(classOf[PostPublishEventRouter])
  val mapType: Type = new TypeToken[java.util.Map[String, AnyRef]]() {}.getType
  val contentTypes = List("Course")
  private var cache: DataCache = _
  private var contentCache: DataCache = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
    neo4JUtil = new Neo4JUtil(config.graphRoutePath, config.graphName)
    val redisConnect = new RedisConnect(config)
    cache = new DataCache(config, redisConnect, config.collectionCacheStore, List())
    cache.init()

    val metaRedisConn = new RedisConnect(config, Option(config.metaRedisHost), Option(config.metaRedisPort))
    contentCache = new DataCache(config, metaRedisConn, config.contentCacheStore, List())
    contentCache.init()
  }

  override def close(): Unit = {
    cassandraUtil.close()
    super.close()
  }

  override def processElement(event: Event, context: ProcessFunction[Event, String]#Context, metrics: Metrics): Unit = {
    logger.info("Processed event using JobRequest-SerDe: " + event)
    metrics.incCounter(config.totalEventsCount)
    if (event.validEvent()) {
      val identifier = event.collectionId

      // Process Shallow Copy Content
      getShallowCopiedContents(identifier)(config, httpUtil).foreach(metadata => context.output(config.shallowContentPublishOutTag, metadata))

      // Process Batch Creation
      val batchDetails = getBatchDetails(identifier)(neo4JUtil, cassandraUtil, config)
      if (!batchDetails.isEmpty)
        context.output(config.batchCreateOutTag, batchDetails)

      // Process Dialcode link
      val dialCodeDetails = getDialCodeDetails(identifier, event)(neo4JUtil, config)
      if (!dialCodeDetails.isEmpty)
        context.output(config.linkDIALCodeOutTag, dialCodeDetails)

      //Process Post Publish Relation Update
      val postPublishRelationUpdateDetails: java.util.Map[String, AnyRef] = getPrimaryCategory(identifier, event)(metrics, config, httpUtil,cache,contentCache)
      if(!postPublishRelationUpdateDetails.isEmpty)
        context.output(config.postPublishRelationUpdateOutTag,postPublishRelationUpdateDetails)

    } else {
      metrics.incCounter(config.skippedEventCount)
      logger.info(s"Event not qualified for publishing for Identifier : ${event.collectionId}.")
    }
  }

  override def metricsList(): List[String] = {
    List(config.skippedEventCount, config.totalEventsCount)
  }
}
