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
import org.sunbird.job.util.{CassandraUtil, HTTPResponse, HttpUtil, JSONUtil, ScalaJsonUtil}
import org.sunbird.job.{BaseProcessFunction, Metrics}

import java.time.format.DateTimeFormatter
import java.time.{ZoneId, ZonedDateTime}
import java.util
import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.{`collection AsScalaIterable`, `seq AsJavaList`}
import scala.collection.mutable.ListBuffer

/** @author
  *   mahesh.vakkund
  */
class PostPublishRelationUpdaterFunction(
    config: PostPublishProcessorConfig,
    httpUtil: HttpUtil,
    @transient var cassandraUtil: CassandraUtil = null
) extends BaseProcessFunction[String, String](config)
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
      identifier: String
  )(implicit
      config: PostPublishProcessorConfig,
      httpUtil: HttpUtil,
      cassandraUtil: CassandraUtil,
      metrics: Metrics
  ): Unit = {
    val programHierarchy = getProgramHierarchy(
      identifier
    )(metrics, config, cache, httpUtil)
    if (programHierarchy.isEmpty) {
      logger.info(
        "PostPublishRelationUpdaterFunction :: Failed to get program Hierarchy."
      )
      return
    }
    addFirstChildId(identifier, programHierarchy) (config, httpUtil, metrics)
    val childrenList = programHierarchy.get(config.children).asInstanceOf[java.util.List[java.util.HashMap[String, AnyRef]]]
    for (childNode <- childrenList) {
      val primaryCategory: String = childNode.get(config.primaryCategory).asInstanceOf[String]
        val childId: String = childNode.get("identifier").asInstanceOf[String]
        if (primaryCategory.equalsIgnoreCase("Course")) {
          val contentObj: java.util.Map[String, AnyRef] = getCourseInfo(childId)(metrics, config, cache, httpUtil)
          var versionKey: String = contentObj.getOrDefault(config.versionKey, "").asInstanceOf[String]
          logger.info("Child Course Id: " + childId + ", Info: " + JSONUtil.serialize(contentObj))

          // Use Option to safely handle null values
          val parentCollections: List[String] = Option(contentObj.get(config.parentCollections))
            .collect {
              case list: java.util.List[_] =>
                list.asInstanceOf[java.util.List[String]].asScala.toList
              case list: List[_] => 
                list.asInstanceOf[List[String]]
            }
            .getOrElse(List.empty)
          logger.info("Child course Id: " + childId + ", existing parentCollections: " + JSONUtil.serialize(parentCollections))
          
          // Update parentCollections if identifier is not present
          val updatedParentCollections: List[String] = if (!parentCollections.contains(identifier)) {
            parentCollections :+ identifier
          } else {
            parentCollections
          }

          logger.info("Child course Id: " + childId + ", updated parentCollections: " + JSONUtil.serialize(updatedParentCollections))
          
          if (updatedParentCollections.size != parentCollections.size) {
            logger.info("ParentCollections is updated for course, calling system update API.")
            val requestData: Map[String, Any] = Map(
              "request" -> Map(
                "content" -> Map(
                  "versionKey" -> versionKey,
                  "parentCollections" -> updatedParentCollections
                )
            ))
            val jsonString: String = JSONUtil.serialize(requestData)
            logger.info("Calling content update with body: " + jsonString)
            val patchRequest = new HttpPatch(
              config.contentSystemUpdatePath + childId
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
          } else {
            logger.info("ParentCollections is not updated. Ignoring system update API.")
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
      .from(config.hierarchyStoreKeySpace, config.contentHierarchyTable)
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
      identifier: String,
      context: ProcessFunction[String, String]#Context,
      metrics: Metrics
  ): Unit = {
    val isValidProgram: Boolean =
      verifyPrimaryCategory(identifier)(metrics, config, httpUtil, cache)
    if (isValidProgram) {
      metrics.incCounter(config.postPublishRelationUpdateEventCount)
      logger.info(
        "PostPublishRelationUpdaterFunction:: started for Content : " + identifier
      )
      try {
        postPublishRelationUpdate(identifier)(
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
      val courseHierarchy = getProgramHierarchy(
        identifier
      )(metrics, config, cache, httpUtil)
      if (courseHierarchy.isEmpty) {
        logger.info(
          "PostPublishRelationUpdaterFunction :: Failed to get program Hierarchy."
        )
        return
      }
      addFirstChildId(identifier, courseHierarchy) (config, httpUtil, metrics)
      /*logger.info(
        "PostPublishRelationUpdaterFunction:: Nothing to do for ContentId : " + identifier
      )*/
    }
  }

  override def metricsList(): List[String] = {
    List(config.postPublishRelationUpdateEventCount, 
        config.postPublishRelationUpdateSuccessCount, 
        config.postPublishRelationUpdateFailureCount)
  }

  private def addFirstChildId(identifier: String, programHierarchy: java.util.Map[String, AnyRef])(
    config: PostPublishProcessorConfig,
    httpUtil: HttpUtil,
    metrics: Metrics
  ): Map[String, AnyRef] = {
    val headers = Map[String, String](
      config.HEADER_CONTENT_TYPE_KEY -> config.HEADER_CONTENT_TYPE_JSON
    )
    val res = executeHttpRequest(config.chRead.replace(":id", identifier), "", headers, "GET")(config, httpUtil, metrics)
    val versionKey = res.get("content").asInstanceOf[Some[Map[String, AnyRef]]].get("versionKey").asInstanceOf[String]
    val firstChild = findFirstLearningResourceIdentifier(programHierarchy.asInstanceOf[util.HashMap[String, AnyRef]]).asInstanceOf[Some[String]].get
    logger.info("Adding FirstChild:" + firstChild + " for ContentId : " + identifier)
    var body = "{\"request\":{\"content\":{\"versionKey\":\"$version\",\"firstChildId\":\"$firstChildId\"}}}"
    body = body.replace("$version", versionKey)
    body = body.replace("$firstChildId", firstChild)
    logger.info("Adding body:" + body + " for ContentId : " + identifier)
    executeHttpRequest(config.contentSystemUpdatePath + identifier, body, headers, "PATCH")(config, httpUtil, metrics)
  }

  def findFirstLearningResourceIdentifier(data: java.util.HashMap[String, AnyRef]): Option[String] = {
    val scalaMap: Map[String, AnyRef] = data.asScala.toMap
    findFirstLearningResourceIdentifierHelper(scalaMap)
  }

  private def findFirstLearningResourceIdentifierHelper(data: Map[String, AnyRef]): Option[String] = {
    data.get("primaryCategory") match {
      case Some(category: String) if category == "Learning Resource" =>
        Some(data("identifier").asInstanceOf[String])
      case _ =>
        val children = data.getOrElse("children", new java.util.ArrayList[java.util.HashMap[String, AnyRef]]())
          .asInstanceOf[java.util.List[java.util.HashMap[String, AnyRef]]]
          .asScala.toList
        children
          .view
          .flatMap(child => findFirstLearningResourceIdentifierHelper(child.asScala.toMap))
          .headOption
    }
  }

  private def executeHttpRequest(url: String, body: String, headers: Map[String, String], method: String)(
    config: PostPublishProcessorConfig,
    httpUtil: HttpUtil,
    metrics: Metrics
  ): Map[String, AnyRef] = {
    val response: HTTPResponse = method.toUpperCase match {
      case "GET" => httpUtil.get(url, headers)
      case "PATCH" => httpUtil.patch(url, body, headers)
      case _ => throw new Exception()
    }
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

}