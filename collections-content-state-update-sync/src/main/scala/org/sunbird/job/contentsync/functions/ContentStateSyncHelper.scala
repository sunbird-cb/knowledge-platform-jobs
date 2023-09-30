package org.sunbird.job.contentsync.functions

import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.cache.DataCache
import org.sunbird.job.contentsync.task.CollectionsContentStateSyncConfig
import org.sunbird.job.util.{HttpUtil, ScalaJsonUtil}

trait ContentStateSyncHelper {

  private[this] val logger = LoggerFactory.getLogger(classOf[CollectionsContentStateSyncFn])
  def getAPICall(url: String, responseParam: String)(config: CollectionsContentStateSyncConfig, httpUtil: HttpUtil, metrics: Metrics): Map[String, AnyRef] = {
    val response = httpUtil.get(url, config.defaultHeaders)
    if (200 == response.status) {
      ScalaJsonUtil.deserialize[Map[String, AnyRef]](response.body)
        .getOrElse("result", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
        .getOrElse(responseParam, Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
    } else if (400 == response.status && response.body.contains(config.userAccBlockedErrCode)) {
      metrics.incCounter(config.skippedEventCount)
      logger.error(s"Error while fetching user details for ${url}: " + response.status + " :: " + response.body)
      Map[String, AnyRef]()
    } else {
      throw new Exception(s"Error from get API : ${url}, with response: ${response}")
    }
  }

  def getCourseReadDetails(courseId: String)(metrics: Metrics, config: CollectionsContentStateSyncConfig, cache: DataCache, httpUtil: HttpUtil): java.util.ArrayList[String] = {
    val courseMetadata = cache.getWithRetry(courseId)
    if (null == courseMetadata || courseMetadata.isEmpty) {
      val url = config.contentBasePath + config.contentReadApi + "/" + courseId + "?fields=primaryCategory,identifier,parentCollections"
      val response = getAPICall(url, "content")(config, httpUtil, metrics)
      response.getOrElse(config.parentCollections, new java.util.ArrayList()).asInstanceOf[java.util.ArrayList[String]]
    } else {
      courseMetadata.getOrElse(config.parentCollections.toLowerCase(), new java.util.ArrayList()).asInstanceOf[java.util.ArrayList[String]]
    }
  }
}
