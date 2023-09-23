package org.sunbird.job.postpublish.helpers

import org.apache.flink.configuration.Configuration
import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.cache.{DataCache, RedisConnect}
import org.sunbird.job.exception.APIException
import org.sunbird.job.postpublish.domain.Event
import org.sunbird.job.postpublish.task.PostPublishProcessorConfig
import org.sunbird.job.util._

/** @author
  *   mahesh.vakkund
  */
trait PostPublishRelationUpdater {

  private[this] val logger =
    LoggerFactory.getLogger(classOf[PostPublishRelationUpdater])

  def getPrimaryCategory(identifier: String)(
      metrics: Metrics,
      config: PostPublishProcessorConfig,
      httpUtil: HttpUtil,
      cache: DataCache
  ): Boolean = {
    logger.info(
      "Verify Program post-publish required for content: " + identifier
    )
    // Get the primary Categories for the courses here
    var isValidProgram = false
    val contentObj: java.util.Map[String, AnyRef] =
      getCourseInfo(identifier)(metrics, config, cache, httpUtil)
    if (contentObj.isEmpty) {
      contentObj.get("primaryCategory") match {
        case Some("Program" | "Curated Program" | "Blended Program") =>
          isValidProgram = true
      }
    }
    isValidProgram
  }

  def getCourseInfo(courseId: String)(
      metrics: Metrics,
      config: PostPublishProcessorConfig,
      cache: DataCache,
      httpUtil: HttpUtil
  ): java.util.Map[String, AnyRef] = {
    val courseMetadata = cache.getWithRetry(courseId)
    if (null == courseMetadata || courseMetadata.isEmpty) {
      val url =
        config.contentReadURL + "/" + courseId + "?fields=identifier,name,versionKey,parentCollections,primaryCategory"
      val response = getAPICall(url, "content")(config, httpUtil, metrics)
      val courseName = StringContext
        .processEscapes(
          response.getOrElse(config.name, "").asInstanceOf[String]
        )
        .filter(_ >= ' ')
      val primaryCategory = StringContext
        .processEscapes(
          response.getOrElse(config.primaryCategory, "").asInstanceOf[String]
        )
        .filter(_ >= ' ')
      val parentCollections = response
        .getOrElse("parentCollections", List.empty[String])
        .asInstanceOf[List[String]]
      val courseInfoMap: java.util.Map[String, AnyRef] =
        new java.util.HashMap[String, AnyRef]()
      courseInfoMap.put("courseId", courseId)
      courseInfoMap.put("courseName", courseName)
      courseInfoMap.put("parentCollections", parentCollections)
      courseInfoMap.put("primaryCategory", primaryCategory)
      courseInfoMap
    } else {
      val courseName = StringContext
        .processEscapes(
          courseMetadata.getOrElse(config.name, "").asInstanceOf[String]
        )
        .filter(_ >= ' ')
      val primaryCategory = StringContext
        .processEscapes(
          courseMetadata
            .getOrElse(config.primaryCategory, "")
            .asInstanceOf[String]
        )
        .filter(_ >= ' ')
      val parentCollections = courseMetadata
        .getOrElse("parentCollections", List.empty[String])
        .asInstanceOf[List[String]]
      val courseInfoMap: java.util.Map[String, AnyRef] =
        new java.util.HashMap[String, AnyRef]()
      courseInfoMap.put("courseId", courseId)
      courseInfoMap.put("courseName", courseName)
      courseInfoMap.put("parentCollections", parentCollections)
      courseInfoMap.put("primaryCategory", primaryCategory)
      courseInfoMap
    }

  }

  def getAPICall(url: String, responseParam: String)(
      config: PostPublishProcessorConfig,
      httpUtil: HttpUtil,
      metrics: Metrics
  ): Map[String, AnyRef] = {
    val response = httpUtil.get(url, config.defaultHeaders)
    if (200 == response.status) {
      ScalaJsonUtil
        .deserialize[Map[String, AnyRef]](response.body)
        .getOrElse("result", Map[String, AnyRef]())
        .asInstanceOf[Map[String, AnyRef]]
        .getOrElse(responseParam, Map[String, AnyRef]())
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

}
