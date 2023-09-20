package org.sunbird.job.programcert.functions

import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.{Row, TypeTokens}
import com.google.common.reflect.TypeToken
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
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

    override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
        val redisConnect = new RedisConnect(config)
        cache = new DataCache(config, redisConnect, config.collectionCacheStore, List())
        cache.init()

      val metaRedisConn = new RedisConnect(config, Option(config.metaRedisHost), Option(config.metaRedisPort))
      contentCache = new DataCache(config, metaRedisConn, config.contentCacheStore, List())
      contentCache.init()
        logger.info("This is the flink testing")
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
            val getParentForCourse = getCourseReadDetails(event.courseId)(metrics, config, contentCache, httpUtil).asInstanceOf[List[String]]
            if(!getParentForCourse.isEmpty) {
                for (parentCourse <- getParentForCourse) {
                    val childrenForCuratedProgram = getProgramChildren(parentCourse)(metrics, config, contentCache, httpUtil).asInstanceOf[Map[String, AnyRef]]
                    if (!childrenForCuratedProgram.isEmpty) {
                        val contentDataForProgram = childrenForCuratedProgram.get(config.childrens).asInstanceOf[List[Map[String, AnyRef]]]
                        var isProgramCertificateToBeGenerated: Boolean = true;
                        for (childNode <- contentDataForProgram) {
                            val courseId: String = childNode.get(config.identifier).asInstanceOf[String]
                            val batchesForCourse: java.util.List[java.util.Map[String, AnyRef]] = childNode.get(config.batches).asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]
                            val filteredBatches = batchesForCourse.filter(batch => batch.get("status") != 2).toList
                            val batchId: String = filteredBatches.get(0).get("batchId").asInstanceOf[String]
                            val userId: String = event.userId
                            val primaryFields = Map(config.dbUserId.toLowerCase() -> userId, config.dbBatchId.toLowerCase -> batchId, config.dbCourseId.toLowerCase -> courseId)
                            val isCertificateIssued: List[Row] = getCourseIssuedCertificateForUser(primaryFields)(metrics).asInstanceOf[List[Row]]
                            breakable {
                                if (isCertificateIssued == null) {
                                    isProgramCertificateToBeGenerated = false;
                                    break
                                }
                            }
                        }
                        if(isProgramCertificateToBeGenerated) {
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
                                    "id" -> event.batchId.concat("_").concat(parentCourse),
                                    "type" -> "ProgramCertificateGeneration"),
                                "edata" -> Map(
                                    "userIds" -> List(event.userId),
                                    "action" -> "issue-certificate",
                                    "iteration" -> 1,
                                    "trigger" -> "auto-issue",
                                    "batchId" -> event.batchId,
                                    "reIssue" -> false,
                                    "courseId" -> parentCourse))

                            val requestBody = JSONUtil.serialize(eData)
                            context.output(config.generateCertificateOutputTag, requestBody)
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

    def getProgramChildren(programId: String)(metrics: Metrics, config: ProgramCertPreProcessorConfig, cache: DataCache, httpUtil: HttpUtil):  Map[String, AnyRef] = {
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

}
