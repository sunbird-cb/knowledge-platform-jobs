package org.sunbird.job.programaggregate.functions

import com.datastax.driver.core.querybuilder.{QueryBuilder, Select}
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import com.twitter.storehaus.cache.TTLCache
import com.twitter.util.Duration
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.cache.{DataCache, RedisConnect}
import org.sunbird.job.dedup.DeDupEngine
import org.sunbird.job.programaggregate.common.DeDupHelper
import org.sunbird.job.programaggregate.task.ProgramActivityAggregateUpdaterConfig
import org.sunbird.job.util.{CassandraUtil, HttpUtil, ScalaJsonUtil}
import org.sunbird.job.{BaseProcessFunction, Metrics}

import java.lang.reflect.Type
import java.util
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.collection.mutable

class ProgramContentConsumptionDeDupFunction(config: ProgramActivityAggregateUpdaterConfig, httpUtil: HttpUtil, @transient var cassandraUtil: CassandraUtil = null)(implicit val stringTypeInfo: TypeInformation[String]) extends BaseProcessFunction[util.Map[String, AnyRef], String](config) {

  val mapType: Type = new TypeToken[Map[String, AnyRef]]() {}.getType
  private[this] val logger = LoggerFactory.getLogger(classOf[ProgramContentConsumptionDeDupFunction])
  var deDupEngine: DeDupEngine = _
  private var cache: DataCache = _
  private var collectionStatusCache: TTLCache[String, String] = _
  lazy private val gson = new Gson()

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
    cache = new DataCache(config, new RedisConnect(config), config.nodeStore, List())
    cache.init()
    collectionStatusCache = TTLCache[String, String](Duration.apply(config.statusCacheExpirySec, TimeUnit.SECONDS))
    deDupEngine = new DeDupEngine(config, new RedisConnect(config, Option(config.deDupRedisHost), Option(config.deDupRedisPort)), config.deDupStore, config.deDupExpirySec)
    deDupEngine.init()
  }

  override def close(): Unit = {
    if (cassandraUtil != null) {
      cassandraUtil.close()
    }
    if (cache != null) {
      cache.close()
    }
    deDupEngine.close()
    super.close()
  }

  override def processElement(event: util.Map[String, AnyRef], context: ProcessFunction[util.Map[String, AnyRef], String]#Context, metrics: Metrics): Unit = {
    metrics.incCounter(config.totalEventCount)
    val eData = event.get(config.eData).asInstanceOf[util.Map[String, AnyRef]].asScala
    val isBatchEnrollmentEvent: Boolean = StringUtils.equalsIgnoreCase(eData.getOrElse(config.action, "").asInstanceOf[String], config.batchEnrolmentUpdateCode)
    if (isBatchEnrollmentEvent) {
      val contents = eData.getOrElse(config.contents, new util.ArrayList[java.util.Map[String, AnyRef]]()).asInstanceOf[util.List[java.util.Map[String, AnyRef]]].asScala
      logger.info("Input Event: " + contents)
      var updatedEventInfo: mutable.ListBuffer[Map[String, AnyRef]] = mutable.ListBuffer.empty[Map[String, AnyRef]]
      var eventInfoMap: mutable.Iterable[Map[String, AnyRef]] = getProgramEvent(eData.toMap)(metrics, config, httpUtil, cache)
      logger.info("EventInfoMap: " + eventInfoMap)
      if (eventInfoMap != null) {
        updatedEventInfo ++= eventInfoMap
      }

      logger.info("UpdatedEventInfoMap: " + updatedEventInfo)

     updatedEventInfo.filter(e => discardDuplicates(e)).foreach(d => context.output(config.uniqueConsumptionOutput, d))
    } else metrics.incCounter(config.skipEventsCount)
  }

  override def metricsList(): List[String] = {
    List(config.totalEventCount, config.skipEventsCount, config.batchEnrolmentUpdateEventCount, config.dbReadCount)
  }

  def discardDuplicates(event: Map[String, AnyRef]): Boolean = {
    if (config.dedupEnabled) {
      val userId = event.getOrElse(config.userId, "").asInstanceOf[String]
      val courseId = event.getOrElse(config.courseId, "").asInstanceOf[String]
      val batchId = event.getOrElse(config.batchId, "").asInstanceOf[String]
      val contents = event.getOrElse(config.contents, List[Map[String,AnyRef]]()).asInstanceOf[List[Map[String, AnyRef]]]
      if (contents.nonEmpty) {
        val content = contents.head
        val contentId = content.getOrElse("contentId", "").asInstanceOf[String]
        val status = content.getOrElse("status", 0.asInstanceOf[AnyRef]).asInstanceOf[Number].intValue()
        val checksum = DeDupHelper.getMessageId(courseId, batchId, userId, contentId, status)
        deDupEngine.isUniqueEvent(checksum)
      } else false
    } else true
  }

  def getProgramEvent(eventData: Map[String, AnyRef])(
    metrics: Metrics,
    config: ProgramActivityAggregateUpdaterConfig,
    httpUtil: HttpUtil,
    cache: DataCache
  ): mutable.Iterable[Map[String, AnyRef]] = {
    logger.info("EventInfo" + eventData)
    var eventInfoMap: mutable.ListBuffer[Map[String, AnyRef]] = mutable.ListBuffer.empty[Map[String, AnyRef]]
    val userId: String = eventData.getOrElse(config.userId, "").asInstanceOf[String]
    val courseId: String = eventData.getOrElse(config.courseId, "").asInstanceOf[String]
    val batchId: String = eventData.getOrElse(config.batchId, "").asInstanceOf[String]
    val contentObj: java.util.Map[String, AnyRef] = getCourseInfo(courseId)(metrics, config, cache, httpUtil)
    val primaryCategory: String = contentObj.get(config.primaryCategory).asInstanceOf[String]
    val parentCollections: List[String] = contentObj.get(config.parentCollections).asInstanceOf[List[String]]
    logger.info("Inside Process Method" + primaryCategory + " ParentCollections: " + parentCollections)
    if (config.validProgramPrimaryCategory.contains(primaryCategory)) {
      eventInfoMap += eventData
    } else if (("Course".equalsIgnoreCase(primaryCategory) || ("Standalone Assessment".equalsIgnoreCase(primaryCategory)))
      && !parentCollections.isEmpty) {
      for (parentId <- parentCollections) {
        val row = getEnrolment(userId, parentId)(metrics)
        if (row != null) {
          val contentConsumption = eventData.getOrElse(config.contents, new util.ArrayList[java.util.Map[String, AnyRef]]()).asInstanceOf[util.List[java.util.Map[String, AnyRef]]].asScala.map(_.asScala.toMap)
          val filteredContents = contentConsumption.filter(x => x.get("status") == 2)
          val eventInfoProgram = Map[String, AnyRef]("contents" -> filteredContents.toList,
            "userId" -> userId,
            "action" -> "batch-enrolment-update",
            "iteration" -> 1.asInstanceOf[Integer],
            "batchId" -> row.getString("batchid"),
            "courseId" -> parentId)
          eventInfoMap += eventInfoProgram
          logger.info("EventMapInfoProgram:" + eventInfoProgram)
        }
      }
    } else {
      logger.error("Not Valid Primary Category: " + primaryCategory + " parentCollections: " + parentCollections)
    }
    eventInfoMap
  }

  def getEnrolment(userId: String, courseId: String)(implicit metrics: Metrics) = {
    val selectWhere: Select.Where = QueryBuilder.select().all()
      .from(config.dbKeyspace, config.dbUserEnrolmentsTable).
      where()
    selectWhere.and(QueryBuilder.eq("userid", userId))
      .and(QueryBuilder.eq("courseid", courseId))
    metrics.incCounter(config.dbReadCount)
    cassandraUtil.findOne(selectWhere.toString)
  }

  def getCourseInfo(courseId: String)(
    metrics: Metrics,
    config: ProgramActivityAggregateUpdaterConfig,
    cache: DataCache,
    httpUtil: HttpUtil
  ): java.util.Map[String, AnyRef] = {
    val courseMetadata = cache.getWithRetry(courseId)
    if (null == courseMetadata || courseMetadata.isEmpty) {
      val url =
        config.contentReadURL + courseId + "?fields=identifier,name,primaryCategory,parentCollections"
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
        .getOrElse(config.parentCollections, List.empty[String]).asInstanceOf[List[String]]
      val courseInfoMap: java.util.Map[String, AnyRef] =
        new java.util.HashMap[String, AnyRef]()
      courseInfoMap.put("courseId", courseId)
      courseInfoMap.put("courseName", courseName)
      courseInfoMap.put("primaryCategory", primaryCategory)
      courseInfoMap.put("parentCollections", parentCollections)
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
        .getOrElse(config.parentCollections, List.empty[String]).asInstanceOf[List[String]]
      val courseInfoMap: java.util.Map[String, AnyRef] =
        new java.util.HashMap[String, AnyRef]()
      courseInfoMap.put("courseId", courseId)
      courseInfoMap.put("courseName", courseName)
      courseInfoMap.put("primaryCategory", primaryCategory)
      courseInfoMap.put("parentCollections", parentCollections)
      courseInfoMap
    }

  }

  def getAPICall(url: String, responseParam: String)(
    config: ProgramActivityAggregateUpdaterConfig,
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
