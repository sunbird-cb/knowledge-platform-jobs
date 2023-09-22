package org.sunbird.job.postpublish.helpers

import org.slf4j.LoggerFactory
import org.sunbird.job.exception.APIException
import org.sunbird.job.postpublish.domain.Event
import org.sunbird.job.postpublish.task.PostPublishProcessorConfig
import org.sunbird.job.util._

/**
 * @author mahesh.vakkund
 */
trait PostPublishRelationUpdate {

  private[this] val logger = LoggerFactory.getLogger(classOf[PostPublishRelationUpdate])

  def getPrimaryCategory(identifier: String, event: Event)(config: PostPublishProcessorConfig, httpUtil: HttpUtil): java.util.Map[String, AnyRef] = {
    logger.info("Process Batch Creation for content: " + identifier)
    // Get the primary Categories for the courses here
    val contentObj: java.util.Map[String, AnyRef] = getContentMetaData(identifier, httpUtil, config)
    if (contentObj.isEmpty) {
      contentObj.get("primaryCategory") match {
        case Some("Program" | "Curated Program" | "Blended Program") => contentObj
        case _ => new java.util.HashMap[String,AnyRef]()
      }
    } else {
      new
          java.util.HashMap[String, AnyRef]()
    }
  }

  /**
   * Get the Content Metadata and return the parsed metadata map
   *
   * @param identifier Content ID
   * @param httpUtil  HttpUil instance
   * @param config    Config instance
   * @return parsed metadata map
   */
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

  def getCourseInfo(courseId: String)(metrics: Metrics, config: CollectionCertPreProcessorConfig, cache: DataCache, httpUtil: HttpUtil): Map[String, Any] = {
        val courseMetadata = cache.getWithRetry(courseId)
        val courseInfoMap = if (null == courseMetadata || courseMetadata.isEmpty) {
            val url = config.contentBasePath + config.contentReadURL + "/" + courseId + "?fields=name,parentCollections,primaryCategory"
            val response = getAPICall(url, "content")(config, httpUtil, metrics)
            val courseName = StringContext.processEscapes(response.getOrElse(config.name, "").asInstanceOf[String]).filter(_ >= ' ')
            val primaryCategory = StringContext.processEscapes(response.getOrElse(config.primaryCategory, "").asInstanceOf[String]).filter(_ >= ' ')
            val parentCollections = response.getOrElse("parentCollections", List.empty[String]).asInstanceOf[List[String]]
            Map(
                "courseId" -> courseId, 
                "courseName" -> courseName, 
                "parentCollections" -> parentCollections,
                "primaryCategory" -> primaryCategory
            )
        } else {
            val courseName = StringContext.processEscapes(courseMetadata.getOrElse(config.name, "").asInstanceOf[String]).filter(_ >= ' ')
            val primaryCategory = StringContext.processEscapes(courseMetadata.getOrElse(config.primaryCategory, "").asInstanceOf[String]).filter(_ >= ' ')
            val parentCollections = courseMetadata.getOrElse("parentCollections", List.empty[String]).asInstanceOf[List[String]]
            Map(
                "courseId" -> courseId, 
                "courseName" -> courseName, 
                "parentCollections" -> parentCollections,
                "primaryCategory" -> primaryCategory
            )
        }
        courseInfoMap
    }
}
