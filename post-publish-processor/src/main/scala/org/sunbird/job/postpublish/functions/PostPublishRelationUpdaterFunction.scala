package org.sunbird.job.postpublish.functions

import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.{Row, TypeTokens}
import com.google.common.reflect.TypeToken
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.http.{HttpResponse, StatusLine}
import org.apache.http.client.methods.HttpPatch
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.impl.client.HttpClients
import org.slf4j.LoggerFactory
import org.sunbird.job.cache.DataCache
import org.sunbird.job.exception.APIException
import org.sunbird.job.postpublish.helpers.BatchCreation
import org.sunbird.job.postpublish.task.PostPublishProcessorConfig
import org.sunbird.job.util.{CassandraUtil, HTTPResponse, HttpUtil, JSONUtil}
import org.sunbird.job.{BaseProcessFunction, Metrics}

import java.time.format.DateTimeFormatter
import java.time.{ZoneId, ZonedDateTime}
import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
/**
 * @author mahesh.vakkund
 */
class PostPublishRelationUpdaterFunction(config: PostPublishProcessorConfig, httpUtil: HttpUtil,
                                         @transient var cassandraUtil: CassandraUtil = null)
  extends BaseProcessFunction[java.util.Map[String, AnyRef], String](config) with BatchCreation {

  private[this] val logger = LoggerFactory.getLogger(classOf[PostPublishRelationUpdaterFunction])

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
  }

  override def close(): Unit = {
    cassandraUtil.close()
    super.close()
  }

  private def postPublishRelationUpdate(eData: util.Map[String, AnyRef], startDate: String)(implicit config: PostPublishProcessorConfig, httpUtil: HttpUtil, cassandraUtil: CassandraUtil): Unit = {
    val childrenForCuratedProgram: Map[String, AnyRef] = getProgramChildren(eData.get("identifier").asInstanceOf[String])
    if (childrenForCuratedProgram.nonEmpty) {
      val contentDataForProgram = childrenForCuratedProgram.get(config.childrens).asInstanceOf[List[Map[String, AnyRef]]]
      for (childNode <- contentDataForProgram) {
        val primaryCategory: String = childNode.get(config.primaryCategory).asInstanceOf[String]
        if (primaryCategory.equalsIgnoreCase("Course")) {
          val versionKey: String = childNode.get(config.versionKey).asInstanceOf[String]
          val contentObj: java.util.Map[String, AnyRef] = getContentMetaData(childNode.get("identifier").asInstanceOf[String], httpUtil, config);
          val parentCollections: ListBuffer[String] = contentObj.get(config.parentCollections).asInstanceOf[ListBuffer[String]]
          val identifier: String = eData.get("identifier").asInstanceOf[String]
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
          val content_system_state_url = config.contentSystemUpdatePath + identifier
          val patchRequest = new HttpPatch(content_system_state_url)
          patchRequest.setEntity(new StringEntity(jsonString, ContentType.APPLICATION_JSON))
          val httpClient = HttpClients.createDefault()
          val response: HttpResponse = httpClient.execute(patchRequest)
          val statusLine: StatusLine = response.getStatusLine
          val statusCode: Int = statusLine.getStatusCode
          if (statusCode == 200) {
            logger.info("Processed the request.")
          } else {
            // TO-DO
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

  private def getProgramChildren(programId: String): Map[String, AnyRef] = {
    val query = QueryBuilder.select().all().from(config.contentHierarchyKeySpace, config.contentHierarchyTable)
      .where(QueryBuilder.eq(config.identifier, programId))
    val row: Row = cassandraUtil.findOne(query.toString)
    if (null != row) {
      val templates = row.getMap(config.Hierarchy, TypeToken.of(classOf[String]), TypeTokens.mapOf(classOf[String], classOf[String]))
      templates.asScala.map(template => (template._1 -> template._2.asScala.toMap)).toMap
    } else {
      Map[String, Map[String, String]]()
    }
  }


  override def processElement(eData: java.util.Map[String, AnyRef], context: ProcessFunction[java.util.Map[String, AnyRef], String]#Context, metrics: Metrics): Unit = {
    val collectionId = eData.getOrDefault("identifier", "")
    metrics.incCounter(config.postPublishRelationUpdateEventCount)
    val startDate = ZonedDateTime.now(ZoneId.of("Asia/Kolkata")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    logger.info("Creating Batch for " + collectionId + " with start date:" + startDate)
    try {
      postPublishRelationUpdate(eData, startDate)(config, httpUtil, cassandraUtil)
      metrics.incCounter(config.postPublishRelationUpdateSuccessCount)
      logger.info("Batch created for " + collectionId)
    } catch {
      case ex: Throwable =>
        logger.error(s"Error while processing message for identifier : ${collectionId}.", ex)
        metrics.incCounter(config.postPublishRelationUpdateFailureCount)
        throw ex
    }
  }

  override def metricsList(): List[String] = ???
}


