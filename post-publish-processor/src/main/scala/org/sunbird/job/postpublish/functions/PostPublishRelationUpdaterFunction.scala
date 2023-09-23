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

/** @author
  *   mahesh.vakkund
  */
class PostPublishRelationUpdaterFunction(
    config: PostPublishProcessorConfig,
    httpUtil: HttpUtil,
    @transient var cassandraUtil: CassandraUtil = null
) extends BaseProcessFunction[java.util.Map[String, AnyRef], String](config)
    with PostPublishRelationUpdater {

  private[this] val logger =
    LoggerFactory.getLogger(classOf[PostPublishRelationUpdaterFunction])
  lazy private val mapper: ObjectMapper = new ObjectMapper()
  private var cache: DataCache = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
    val redisConnect = new RedisConnect(config)
    cache =
      new DataCache(config, redisConnect, config.contentCacheStore, List())
    cache.init()
  }

  override def close(): Unit = {
    cassandraUtil.close()
    cache.close()
    super.close()
  }

  private def postPublishRelationUpdate(
      eData: util.Map[String, AnyRef]
  )(implicit
      config: PostPublishProcessorConfig,
      httpUtil: HttpUtil,
      cassandraUtil: CassandraUtil,
      metrics: Metrics
  ): Unit = {
    val programHierarchy = getProgramHierarchy(
      eData.get("identifier").asInstanceOf[String]
    )(metrics, config, cache, httpUtil)
    if (programHierarchy.isEmpty) {
      logger.info(
        "PostPublishRelationUpdaterFunction :: Failed to get program Hierarchy."
      )
      return
    }

    val childCourseList = programHierarchy
      .get(config.children)
      .asInstanceOf[List[Map[String, AnyRef]]]
    for (childNode <- childCourseList) {
      val primaryCategory: String =
        childNode.get(config.primaryCategory).asInstanceOf[String]
      if (primaryCategory.equalsIgnoreCase("Course")) {
        val contentObj: java.util.Map[String, AnyRef] = getCourseInfo(
          childNode.get("identifier").asInstanceOf[String]
        )(metrics, config, cache, httpUtil)
        val parentCollections: ListBuffer[String] = contentObj
          .getOrDefault(config.parentCollections, ListBuffer.empty[String])
          .asInstanceOf[ListBuffer[String]]
        val versionKey: String =
          contentObj.get(config.versionKey).asInstanceOf[String]
        val identifier: String =
          childNode.get("identifier").asInstanceOf[String]
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
        val patchRequest = new HttpPatch(
          config.contentSystemUpdatePath + identifier
        )
        patchRequest.setEntity(
          new StringEntity(jsonString, ContentType.APPLICATION_JSON)
        )
        val httpClient = HttpClients.createDefault()
        val response: HttpResponse = httpClient.execute(patchRequest)
        val statusLine: StatusLine = response.getStatusLine
        val statusCode: Int = statusLine.getStatusCode
        if (statusCode == 200) {
          logger.info("Processed the request.")
        } else {
          logger.error(
            "Received error response for system update API. Response: " + JSONUtil
              .serialize(response)
          )
        }
      }
    }
  }

  def getProgramHierarchy(programId: String)(
      metrics: Metrics,
      config: PostPublishProcessorConfig,
      cache: DataCache,
      httpUtil: HttpUtil
  ): java.util.Map[String, AnyRef] = {
    val query = QueryBuilder
      .select(config.Hierarchy)
      .from(config.contentHierarchyKeySpace, config.contentHierarchyTable)
      .where(QueryBuilder.eq(config.identifier, programId))
    val row = cassandraUtil.find(query.toString)
    if (CollectionUtils.isNotEmpty(row)) {
      val hierarchy =
        row.asScala.head.getObject(config.Hierarchy).asInstanceOf[String]
      if (StringUtils.isNotBlank(hierarchy))
        mapper.readValue(hierarchy, classOf[java.util.Map[String, AnyRef]])
      else new java.util.HashMap[String, AnyRef]()
    } else new java.util.HashMap[String, AnyRef]()
  }

  override def processElement(
      eData: java.util.Map[String, AnyRef],
      context: ProcessFunction[java.util.Map[String, AnyRef], String]#Context,
      metrics: Metrics
  ): Unit = {
    val identifier = eData.getOrDefault("identifier", "").asInstanceOf[String]
    val isValidProgram: Boolean =
      verifyPrimaryCategory(identifier)(metrics, config, httpUtil, cache)
    if (!isValidProgram) {
      metrics.incCounter(config.postPublishRelationUpdateEventCount)
      logger.info(
        "PostPublishRelationUpdaterFunction:: started for Content : " + identifier
      )
      try {
        postPublishRelationUpdate(eData)(
          config,
          httpUtil,
          cassandraUtil,
          metrics
        )
        metrics.incCounter(config.postPublishRelationUpdateSuccessCount)
        logger.info(
          "PostPublishRelationUpdaterFunction:: Completed for ContentId : " + identifier
        )
      } catch {
        case ex: Throwable =>
          logger.error(
            s"Error while processing message for identifier : ${identifier}.",
            ex
          )
          metrics.incCounter(config.postPublishRelationUpdateFailureCount)
          throw ex
      }
    } else {
      logger.info(
        "PostPublishRelationUpdaterFunction:: Nothing to do for ContentId : " + identifier
      )
    }
  }

  override def metricsList(): List[String] = ???
}