package org.sunbird.job.postpublish.functions

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.{BaseProcessFunction, Metrics}
import org.sunbird.job.postpublish.helpers.BatchCreation
import org.sunbird.job.postpublish.task.PostPublishProcessorConfig
import org.sunbird.job.util.{CassandraUtil, HttpUtil}

import java.time.format.DateTimeFormatter
import java.time.{ZoneId, ZonedDateTime}
import java.util

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


