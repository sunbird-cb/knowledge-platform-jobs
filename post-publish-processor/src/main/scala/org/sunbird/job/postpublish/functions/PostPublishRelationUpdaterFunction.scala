package org.sunbird.job.postpublish.functions

import com.datastax.driver.core.querybuilder.QueryBuilder
import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.http.client.methods.HttpPatch
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.impl.client.HttpClients
import org.apache.http.{HttpResponse, StatusLine}
import org.slf4j.LoggerFactory
import org.sunbird.job.cache.{DataCache, RedisConnect}
import org.sunbird.job.exception.APIException
import org.sunbird.job.postpublish.helpers.PostPublishRelationUpdater
import org.sunbird.job.postpublish.task.PostPublishProcessorConfig
import org.sunbird.job.util.{CassandraUtil, HTTPResponse, HttpUtil, JSONUtil}
import org.sunbird.job.{BaseProcessFunction, Metrics}

import java.time.format.DateTimeFormatter
import java.time.{ZoneId, ZonedDateTime}
import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
/**
 * @author mahesh.vakkund
 */
class PostPublishRelationUpdaterFunction(config: PostPublishProcessorConfig, httpUtil: HttpUtil,
                                         @transient var cassandraUtil: CassandraUtil = null)
  extends BaseProcessFunction[java.util.Map[String, AnyRef], String](config) with PostPublishRelationUpdater {

  private[this] val logger = LoggerFactory.getLogger(classOf[PostPublishRelationUpdaterFunction])
  lazy private val mapper: ObjectMapper = new ObjectMapper()
  private var cache: DataCache = _
  private var contentCache: DataCache = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
    val redisConnect = new RedisConnect(config)
    cache = new DataCache(config, redisConnect, config.collectionCacheStore, List())
    cache.init()

    val metaRedisConn = new RedisConnect(config, Option(config.metaRedisHost), Option(config.metaRedisPort))
    contentCache = new DataCache(config, metaRedisConn, config.contentCacheStore, List())
    contentCache.init()
  }

  override def close(): Unit = {
    cassandraUtil.close()
    cache.close()
    super.close()
  }

  private def postPublishRelationUpdate(eData: util.Map[String, AnyRef], startDate: String)(implicit config: PostPublishProcessorConfig, httpUtil: HttpUtil, cassandraUtil: CassandraUtil,metrics:Metrics): Unit = {
    val programHierarchy = getProgramHierarchy(eData.get("identifier").asInstanceOf[String])(metrics, config, contentCache, httpUtil)
    if (!programHierarchy.isEmpty) {
      val childCourseList = programHierarchy.get(config.children).asInstanceOf[List[Map[String, AnyRef]]]
      for (childNode <- childCourseList) {
        val primaryCategory: String = childNode.get(config.primaryCategory).asInstanceOf[String]
        if (primaryCategory.equalsIgnoreCase("Course")) {
          val contentObj: java.util.Map[String, AnyRef] = getCourseInfo(childNode.get("identifier").asInstanceOf[String])(metrics, config, contentCache, httpUtil)
          val parentCollections: ListBuffer[String] = contentObj.getOrDefault(config.parentCollections, ListBuffer.empty[String]).asInstanceOf[ListBuffer[String]]
          val versionKey: String = contentObj.get(config.versionKey).asInstanceOf[String]
          val identifier: String = childNode.get("identifier").asInstanceOf[String]
          if (parentCollections.isEmpty) {
            parentCollections += identifier
          } else {
            if (!parentCollections.contains(identifier)) {
              parentCollections += identifier
            }
          }
          val requestData: Map[String, Any] = Map(
            "request" -> Map(
              "content" -> Map(
                "versionKey" -> versionKey,
                "parentCollections" -> parentCollections.toList
              )
            )
          )
          val jsonString: String = JSONUtil.serialize(requestData)
          val patchRequest = new HttpPatch(config.contentSystemUpdatePath + identifier)
          patchRequest.setEntity(new StringEntity(jsonString, ContentType.APPLICATION_JSON))
          val httpClient = HttpClients.createDefault()
          val response: HttpResponse = httpClient.execute(patchRequest)
          val statusLine: StatusLine = response.getStatusLine
          val statusCode: Int = statusLine.getStatusCode
          if (statusCode == 200) {
            logger.info("Processed the request.")
          } else {
            logger.error("Received error response for system update API. Response: " + JSONUtil.serialize(response))
          }
        }
      }
    }
  }

  @throws[Exception]
  private def getContentMetaData(identifier: String, httpUtil: HttpUtil, config: PostPublishProcessorConfig): java.util.Map[String, AnyRef] = {
    try {
      val content: HTTPResponse = httpUtil.get(config.contentReadURL + identifier)
      val obj = JSONUtil.deserialize[Map[String, AnyRef]](content.body)
      val contentObj = obj("result").asInstanceOf[Map[String, AnyRef]]("content").asInstanceOf[java.util.Map[String, AnyRef]]
      contentObj
    } catch {
      case e: Exception =>
        throw new APIException(s"Error in getContentMetaData for $identifier - ${e.getLocalizedMessage}", e)
    }
  }

  def getProgramHierarchy(programId: String)(metrics: Metrics, config: PostPublishProcessorConfig, cache: DataCache, httpUtil: HttpUtil): java.util.Map[String, AnyRef] = {
    val query = QueryBuilder.select(config.Hierarchy).from(config.contentHierarchyKeySpace, config.contentHierarchyTable)
      .where(QueryBuilder.eq(config.identifier, programId))
    val row = cassandraUtil.find(query.toString)
    if (CollectionUtils.isNotEmpty(row)) {
      val hierarchy = row.asScala.head.getObject(config.Hierarchy).asInstanceOf[String]
      if (StringUtils.isNotBlank(hierarchy))
        mapper.readValue(hierarchy, classOf[java.util.Map[String, AnyRef]])
      else new java.util.HashMap[String, AnyRef]()
    }
    else new java.util.HashMap[String, AnyRef]()
  }



  override def processElement(eData: java.util.Map[String, AnyRef], context: ProcessFunction[java.util.Map[String, AnyRef], String]#Context, metrics: Metrics): Unit = {
    val collectionId = eData.getOrDefault("identifier", "")
    metrics.incCounter(config.postPublishRelationUpdateEventCount)
    val startDate = ZonedDateTime.now(ZoneId.of("Asia/Kolkata")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    logger.info("Processng Post Publish Relation update for Batch " + collectionId + " with start date:" + startDate)
    try {
      postPublishRelationUpdate(eData, startDate)(config, httpUtil, cassandraUtil,metrics)
      metrics.incCounter(config.postPublishRelationUpdateSuccessCount)
      logger.info("Post Publish Update done for" + collectionId)
    } catch {
      case ex: Throwable =>
        logger.error(s"Error while processing message for identifier : ${collectionId}.", ex)
        metrics.incCounter(config.postPublishRelationUpdateFailureCount)
        throw ex
    }
  }

  override def metricsList(): List[String] = ???
}


