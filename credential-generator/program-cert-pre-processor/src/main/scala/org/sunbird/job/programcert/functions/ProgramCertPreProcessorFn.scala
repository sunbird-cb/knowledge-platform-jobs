package org.sunbird.job.programcert.functions

import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.{QueryBuilder, Select}
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

import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.{`collection AsScalaIterable`, `seq AsJavaList`}
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
            config.cacheHitCount)
    }

    override def processElement(event: Event,
                                context: KeyedProcessFunction[String, Event, String]#Context,
                                metrics: Metrics): Unit = {
        try {
            val getParentIdForCourse = event.parentCollections
            if (!getParentIdForCourse.isEmpty) {
                for (courseParentId <- getParentIdForCourse) {
                    val programEnrollmentStatus = getEnrolment(event.userId, courseParentId)(metrics)
                    //if enrolled into program
                    if (null != programEnrollmentStatus) {
                        val curatedProgramHierarchy = getProgramChildren(courseParentId)(metrics, config, contentCache, httpUtil)
                        if (!curatedProgramHierarchy.isEmpty) {
                            val batchId: String = programEnrollmentStatus.getString(config.dbBatchId)
                            val contentDataForProgram = curatedProgramHierarchy.get(config.childrens).asInstanceOf[java.util.List[java.util.HashMap[String, AnyRef]]]
                            var isProgramCertificateToBeGenerated: Boolean = true;
                            for (childNode <- contentDataForProgram) {
                                val courseId: String = childNode.get(config.identifier).asInstanceOf[String]
                                val userId: String = event.userId
                                val primaryFields = Map(config.dbUserId.toLowerCase() -> userId, config.dbCourseId.toLowerCase -> courseId)
                                val isCertificateIssued: List[Row] = getCourseIssuedCertificateForUser(primaryFields)(metrics)
                                breakable {
                                    if (isCertificateIssued == null || isCertificateIssued.isEmpty) {
                                        isProgramCertificateToBeGenerated = false;
                                        break
                                    }
                                }
                            }
                            if (isProgramCertificateToBeGenerated) {
                                //Add kafka event to generate Certificate for Program
                                val eData = Map[String, AnyRef](
                                    "eid" -> "BE_JOB_REQUEST",
                                    "ets" -> "1695019553226",
                                    "mid" -> "LP.1695019553226.dcfd4458-f7e4-4e23-bfb7-9531ec91a1fe",
                                    "actor" -> Map(
                                        "id" -> "Program Certificate Generator",
                                        "type" -> "System"),
                                    "context" -> Map(
                                        "pdata" -> Map(
                                            "ver" -> "1.0",
                                            "id" -> "org.sunbird.platform")),
                                    "object" -> Map(
                                        "id" -> batchId.concat("_").concat(courseParentId),
                                        "type" -> "ProgramCertificateGeneration"),
                                    "edata" -> Map(
                                        "userIds" -> List(event.userId),
                                        "action" -> "issue-certificate",
                                        "iteration" -> 1,
                                        "trigger" -> "auto-issue",
                                        "batchId" -> batchId,
                                        "reIssue" -> false,
                                        "courseId" -> courseParentId))

                                val requestBody = JSONUtil.serialize(eData)
                                context.output(config.generateCertificateOutputTag, requestBody)
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

    def getProgramChildren(programId: String)(metrics: Metrics, config: ProgramCertPreProcessorConfig, cache: DataCache, httpUtil: HttpUtil):  java.util.Map[String, AnyRef] = {
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

    private def getCourseIssuedCertificateForUser(columns: Map[String, AnyRef])(implicit metrics: Metrics) = {
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
        cassandraUtil.find(selectWhere.toString).asScala.toList
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
            row = row.filter(x => x.getInt("status") != 2).toList
            if (row.size() == 1) {
                row.asScala.get(0)
            } else {
                null
            }
        } else {
            null
        }
    }
}
