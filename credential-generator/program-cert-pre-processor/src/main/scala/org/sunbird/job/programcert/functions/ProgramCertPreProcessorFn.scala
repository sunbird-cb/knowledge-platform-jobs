package org.sunbird.job.programcert.functions

import com.datastax.driver.core.{Row, TypeTokens}
import com.datastax.driver.core.querybuilder.{QueryBuilder, Select, Update}
import com.google.common.reflect.TypeToken
import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.cache.{DataCache, RedisConnect}
import org.sunbird.job.exception.InvalidEventException
import org.sunbird.job.programcert.domain.Event
import org.sunbird.job.programcert.task.ProgramCertPreProcessorConfig
import org.sunbird.job.util.{CassandraUtil, HttpUtil, JSONUtil}
import org.sunbird.job.{BaseProcessKeyedFunction, Metrics}

import java.util.{Date, UUID}
import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.{`collection AsScalaIterable`, `seq AsJavaList`}
import scala.collection.mutable
import scala.util.control.Breaks.{break, breakable}

class ProgramCertPreProcessorFn(config: ProgramCertPreProcessorConfig, httpUtil: HttpUtil)
                               (implicit val stringTypeInfo: TypeInformation[String],
                                @transient var cassandraUtil: CassandraUtil = null)
  extends BaseProcessKeyedFunction[String, Event, String](config) with IssueCertificateHelper {

  private[this] val logger = LoggerFactory.getLogger(classOf[ProgramCertPreProcessorFn])
  private var cache: DataCache = _
  private var contentCache: DataCache = _
  lazy private val mapper: ObjectMapper = new ObjectMapper()

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

  override def metricsList(): List[String] = {
    List(config.totalEventsCount, config.dbReadCount, config.dbUpdateCount, config.failedEventCount, config.skippedEventCount, config.successEventCount,
      config.cacheHitCount, config.programCertIssueEventsCount)
  }

  override def processElement(event: Event,
                              context: KeyedProcessFunction[String, Event, String]#Context,
                              metrics: Metrics): Unit = {
    try {
      val getParentIdForCourse = event.parentCollections
      if (!getParentIdForCourse.isEmpty) {
        val enrolmentRecords = getAllEnrolments(event.userId)(metrics)
        for (courseParentId <- getParentIdForCourse) {
          val programEnrollmentRow = getEnrollmentRecord(enrolmentRecords, courseParentId)
          //if enrolled into program
          if (programEnrollmentRow.isDefined && programEnrollmentRow.get.getList(config.issuedCertificates, TypeTokens.mapOf(classOf[String], classOf[String])).isEmpty) {
            val programHierarchy = getProgramChildren(courseParentId)(metrics, config, contentCache, httpUtil)
            if (!programHierarchy.isEmpty) {
              val batchId: String = programEnrollmentRow.get.getString(config.dbBatchId)
              val contentDataForProgram = programHierarchy.get(config.childrens).asInstanceOf[java.util.List[java.util.HashMap[String, AnyRef]]]
              val leafNodeMap = mutable.Map[String, Int]()

              var isProgramCertificateToBeGenerated: Boolean = true;
              var programCompletedOn: Date = null
              for (childNode <- contentDataForProgram) {
                val primaryCategory = childNode.get(config.primaryCategory).asInstanceOf[String]
                if (config.allowedPrimaryCategoryForProgram.contains(primaryCategory)) {
                  val courseId: String = childNode.get(config.identifier).asInstanceOf[String]
                  val userId: String = event.userId
                  val courseEnrollmentRow = getEnrollmentRecord(enrolmentRecords, courseId)
                  val isCertificateIssued = courseEnrollmentRow.isDefined && !courseEnrollmentRow.get.getList(config.issuedCertificates, TypeTokens.mapOf(classOf[String], classOf[String])).isEmpty
                  logger.info("Is Certificate Available for courseId: " + courseId + " userId:" + userId + " :" + isCertificateIssued)
                  var courseCompletedOn: Date = null;
                  if (isCertificateIssued) {
                    courseCompletedOn = courseEnrollmentRow.get.getTimestamp("completedon")
                    if (programCompletedOn == null) {
                      programCompletedOn = courseCompletedOn
                    } else if (programCompletedOn.before(courseCompletedOn)) {
                      programCompletedOn = courseCompletedOn
                    }
                  }

                  breakable {
                    if (!isCertificateIssued) {
                      isProgramCertificateToBeGenerated = false;
                      break
                    } else {
                      val leafNodes = childNode.get(config.leafNodes).asInstanceOf[java.util.List[String]]
                      for (leafNode <- leafNodes) {
                        leafNodeMap += (leafNode -> 2)
                      }
                    }
                  }
                }
              }
              if (!leafNodeMap.isEmpty) {
                val programContentStatus = Option(programEnrollmentRow.get.getMap(
                  config.contentStatus, TypeToken.of(classOf[String]), TypeToken.of(classOf[Integer]))).head
                var progressCount: Integer = Option(programEnrollmentRow.get.getInt(config.progress)).head

                var updateCount = 0

                for ((key, value) <- leafNodeMap) {
                  // Check if the key is present in leafNodeMap
                  if (programContentStatus.get(key) != null) {
                    if (programContentStatus.get(key) != 2) {
                      // Update progress in contentStatus for the matching key
                      programContentStatus.put(key, value)
                      updateCount += 1
                    }
                  } else {
                    programContentStatus.put(key, value)
                    updateCount += 1
                  }
                }

                // Update the progress with the total update count
                progressCount += updateCount
                var status: Int = 1
                val leafNodesForProgram = programHierarchy.get(config.leafNodes).asInstanceOf[java.util.List[String]]
                if (progressCount == leafNodesForProgram.size()) {
                  status = 2
                } else {
                  isProgramCertificateToBeGenerated = false
                }
                updateEnrolment(event.userId, batchId, courseParentId, programContentStatus, status, progressCount, programCompletedOn)(metrics)
              }

              if (isProgramCertificateToBeGenerated) {
                //Add kafka event to generate Certificate for Program
                logger.info("Adding the kafka event for programId: " + courseParentId)
                createIssueCertEventForProgram(courseParentId, event.userId, batchId, context)(metrics)
              }
            }
          }
        }
      }
    } catch {
      case ex: Exception => {
        throw new InvalidEventException(ex.getMessage, Map("partition" -> event.partition, "offset" -> event.offset), ex)
      }
    }
    logger.info("Inside the Process ElementForProgram");
  }

  def getProgramChildren(programId: String)(metrics: Metrics, config: ProgramCertPreProcessorConfig, cache: DataCache, httpUtil: HttpUtil): java.util.Map[String, AnyRef] = {
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

  private def getCourseEnrollment(columns: Map[String, AnyRef])(implicit metrics: Metrics): Row = {
    logger.info("primary columns {}", columns)
    val selectWhere = QueryBuilder.select().all()
      .from(config.keyspace, config.userEnrolmentsTable).
      where()
    columns.map(col => {
      col._2 match {
        case value: List[Any] =>
          selectWhere.and(QueryBuilder.in(col._1, value.asJava))
        case _ =>
          selectWhere.and(QueryBuilder.eq(col._1, col._2))
      }
    })
    logger.info("select query {}", selectWhere.toString)
    var row: java.util.List[Row] = cassandraUtil.find(selectWhere.toString)
    if (null != row) {
      if (row.size() == 1) {
        row.asScala.get(0)
      } else {
        logger.error("More than one certificate" + columns)
        null
      }
    } else {
      logger.error("No Certificate Available" + columns)
      null
    }
  }

  def getEnrolment(userId: String, programId: String)(implicit metrics: Metrics): Row = {
    val selectWhere: Select.Where = QueryBuilder.select().all()
      .from(config.keyspace, config.userEnrolmentsTable).
      where()
    selectWhere.and(QueryBuilder.eq(config.dbUserId, userId))
      .and(QueryBuilder.eq(config.dbCourseId, programId))
    metrics.incCounter(config.dbReadCount)
    var row: java.util.List[Row] = cassandraUtil.find(selectWhere.toString)
    if (null != row) {
      if (row.size() == 1) {
        row.asScala.get(0)
      } else {
        logger.error("Enrollement is more than 1, for programId:" + programId + " userId:" + userId)
        null
      }
    } else {
      logger.error("No Enrollement found for programId: " + programId + " userId: " + userId)
      null
    }
  }

  def updateEnrolment(userId: String, batchId: String, programId: String, contentStatus: java.util.Map[String, Integer], status: Int, progress: Int, programCompletedOn: Date)(implicit metrics: Metrics): Unit = {
    logger.info("Enrolment updated for userId: " + userId + " batchId: " + batchId)
    val updateQuery = QueryBuilder.update(config.keyspace, config.userEnrolmentsTable)
      .`with`(QueryBuilder.set("status", status))
      .and(QueryBuilder.set("progress", progress))
      .and(QueryBuilder.set("contentstatus", contentStatus))
      .and(QueryBuilder.set("datetime", System.currentTimeMillis))
    if (status == 2) {
      updateQuery.and(QueryBuilder.set("completedon", programCompletedOn))
    }
    updateQuery.where(QueryBuilder.eq("userid", userId))
      .and(QueryBuilder.eq("courseid", programId))
      .and(QueryBuilder.eq("batchid", batchId))

    val result = cassandraUtil.upsert(updateQuery.toString)
    if (result) {
      metrics.incCounter(config.dbUpdateCount)
    } else {
      val msg = "Database update has failed" + updateQuery.toString
      logger.error(msg)
      throw new Exception(msg)
    }
  }

  def createIssueCertEventForProgram(programId: String, userId: String, batchId: String, context: KeyedProcessFunction[String, Event, String]#Context)(implicit metrics: Metrics): Unit = {
    val ets = System.currentTimeMillis
    val mid = s"""LP.${ets}.${UUID.randomUUID}"""
    val event = s"""{"eid": "BE_JOB_REQUEST","ets": ${ets},"mid": "${mid}","actor": {"id": "Program Certificate Generator","type": "System"},"context": {"pdata": {"ver": "1.0","id": "org.sunbird.platform"}},"object": {"id": "${batchId}_${programId}","type": "ProgramCertificateGeneration"},"edata": {"userIds": ["${userId}"],"action": "issue-certificate","iteration": 1, "trigger": "auto-issue","batchId": "${batchId}","reIssue": false,"courseId": "${programId}"}}"""
    logger.info("o/p event:  " + event)
    context.output(config.generateCertificateOutputTag, event)
    metrics.incCounter(config.programCertIssueEventsCount)
  }

  def getAllEnrolments(userId: String)(implicit metrics: Metrics): java.util.List[Row] = {
    val selectWhere: Select.Where = QueryBuilder.select(config.dbUserId, config.dbCourseId, config.dbBatchId, config.contentStatus, config.progress, config.issuedCertificates, "completedon")
      .from(config.keyspace, config.userEnrolmentsTable).where()
    selectWhere.and(QueryBuilder.eq(config.dbUserId, userId))
    metrics.incCounter(config.dbReadCount)
    cassandraUtil.find(selectWhere.toString)
  }

  def getEnrollmentRecord(enrollList: java.util.List[Row], courseId: String): Option[Row] = {
    if(null != enrollList) {
      enrollList.asScala.find { row =>
        val courseid = row.getString("courseid")
        val active = row.getBool("active")
        courseid == courseId && active
      }
    } else {
      None
    }
  }
}
