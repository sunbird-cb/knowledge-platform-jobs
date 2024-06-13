package org.sunbird.job.programcert.functions

import java.text.SimpleDateFormat
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.{Row, TypeTokens}
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.cache.DataCache
import org.sunbird.job.programcert.domain.{AssessedUser, AssessmentUserAttempt, EnrolledUser, Event}
import org.sunbird.job.programcert.task.{ProgramCertPreProcessorConfig}
import org.sunbird.job.util.{CassandraUtil, HttpUtil, ScalaJsonUtil}

import scala.collection.JavaConverters._

trait IssueCertificateHelper {

  private[this] val logger = LoggerFactory.getLogger(classOf[ProgramCertPreProcessorFn])
  def getAPICall(url: String, responseParam: String)(config: ProgramCertPreProcessorConfig, httpUtil: HttpUtil, metrics: Metrics): Map[String, AnyRef] = {
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
}
