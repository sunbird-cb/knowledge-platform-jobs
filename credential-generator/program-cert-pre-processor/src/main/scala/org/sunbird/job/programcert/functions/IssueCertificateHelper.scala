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
}
