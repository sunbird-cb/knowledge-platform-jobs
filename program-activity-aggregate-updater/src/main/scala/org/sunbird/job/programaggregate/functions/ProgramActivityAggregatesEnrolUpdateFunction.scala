package org.sunbird.job.programaggregate.functions

import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.{QueryBuilder, Select}
import com.google.common.reflect.TypeToken
import com.google.gson.Gson
import com.twitter.storehaus.cache.TTLCache
import com.twitter.util.Duration
import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.slf4j.LoggerFactory
import org.sunbird.job.cache.{DataCache, RedisConnect}
import org.sunbird.job.programaggregate.domain._
import org.sunbird.job.programaggregate.task.ProgramActivityAggregateUpdaterConfig
import org.sunbird.job.util.{CassandraUtil, HttpUtil}
import org.sunbird.job.{Metrics, WindowBaseProcessFunction}

import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.`seq AsJavaList`
import scala.collection.mutable.ListBuffer



class ProgramActivityAggregatesEnrolUpdateFunction(config: ProgramActivityAggregateUpdaterConfig, httpUtil: HttpUtil, @transient var cassandraUtil: CassandraUtil = null)
                                                  (implicit val stringTypeInfo: TypeInformation[String])
  extends WindowBaseProcessFunction[Map[String, AnyRef], String, Int](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[ProgramActivityAggregatesEnrolUpdateFunction])
  private var cache: DataCache = _
  private var collectionStatusCache: TTLCache[String, String] = _
  lazy private val gson = new Gson()

  override def metricsList(): List[String] = {
    List(config.failedEventCount, config.dbUpdateCount, config.dbReadCount, config.cacheHitCount, config.cacheMissCount, config.processedEnrolmentCount, config.retiredCCEventsCount)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
    cache = new DataCache(config, new RedisConnect(config), config.nodeStore, List())
    cache.init()
    collectionStatusCache = TTLCache[String, String](Duration.apply(config.statusCacheExpirySec, TimeUnit.SECONDS))
  }

  override def close(): Unit = {
    if (cassandraUtil != null) {
      cassandraUtil.close()
    }
    if (cache != null) {
      cache.close()
    }
    super.close()
  }

  override def process(key: Int,
                       context: ProcessWindowFunction[Map[String, AnyRef], String, Int, GlobalWindow]#Context,
                       events: Iterable[Map[String, AnyRef]],
                       metrics: Metrics): Unit = {
    logger.info("Event Info Inside ProgramActivityAggregrator: " + events)
    val inputUserConsumptionList: List[UserContentConsumption] = events
      .groupBy(key => (key.get(config.courseId), key.get(config.batchId), key.get(config.userId)))
      .values.map(value => {
        metrics.incCounter(config.processedEnrolmentCount)
        val batchId = value.head(config.batchId).toString
        val userId = value.head(config.userId).toString
        val courseId = value.head(config.courseId).toString
        logger.info("courseId: " + courseId + " batchId: " + batchId)
        val userConsumedContents = value.head(config.contents).asInstanceOf[List[Map[String, AnyRef]]]
        val enrichedContents = getContentStatusFromEvent(userConsumedContents)
        UserContentConsumption(userId = userId, batchId = batchId, courseId = courseId, enrichedContents)
      }).toList
    logger.info("the input user ConsumptionList:" + inputUserConsumptionList)
    if (inputUserConsumptionList.isEmpty)
      return

    val updateProgramEnrollments = updateProgramEnrollment(inputUserConsumptionList)(metrics)

    val collectionProgressUpdateList = updateProgramEnrollments.filter(progress => !progress.completed)
    context.output(config.collectionUpdateOutputTag, collectionProgressUpdateList)

    logger.info("collectionProgressUpdateList Queries List :" + collectionProgressUpdateList)
    val collectionProgressCompleteList = updateProgramEnrollments.filter(progress => progress.completed)
    context.output(config.collectionCompleteOutputTag, collectionProgressCompleteList)

    logger.info("collectionProgressCompleteList Queries List :" + collectionProgressCompleteList)
  }

  def getContentStatusFromEvent(contents: List[Map[String, AnyRef]]): Map[String, ContentStatus] = {
    val enrichedContents = contents.map(content => {
        (content.getOrElse(config.contentId, "").asInstanceOf[String], content.getOrElse(config.status, 0).asInstanceOf[Number])
      }).filter(t => StringUtils.isNotBlank(t._1) && (t._2.intValue() > 0))
      .map(x => {
        val completedCount = if (x._2.intValue() == 2) 1 else 0
        ContentStatus(x._1, x._2.intValue(), completedCount)
      }).groupBy(f => f.contentId)

    enrichedContents.map(content => {
      val consumedList = content._2
      val finalStatus = consumedList.map(x => x.status).max
      val views = sumFunc(consumedList, (x: ContentStatus) => {
        x.viewCount
      })
      val completion = sumFunc(consumedList, (x: ContentStatus) => {
        x.completedCount
      })
      (content._1, ContentStatus(content._1, finalStatus, completion, views))
    })
  }

  /**
   * Computation of Sum for viewCount and completedCount.
   */
  private def sumFunc(list: List[ContentStatus], valFunc: ContentStatus => Int): Int = list.map(x => valFunc(x)).sum

  def updateProgramEnrollment(events: List[UserContentConsumption])(implicit metrics: Metrics): List[CollectionProgress] = {

    val contentEnrollProgress: List[CollectionProgress] = events.flatMap { collectionProgress =>
      val updatedEnrolContentConsumption = updateEnrolContentConsumption(collectionProgress)(metrics)
      if (updatedEnrolContentConsumption != null)
        programEnrolConsumption(updatedEnrolContentConsumption)(metrics)
      else
        None
    }
    contentEnrollProgress
  }

  def getEnrolment(userId: String, programId: String)(implicit metrics: Metrics): Row = {
    val selectWhere: Select.Where = QueryBuilder.select().all()
      .from(config.dbKeyspace, config.dbUserEnrolmentsTable).
      where()
    selectWhere.and(QueryBuilder.eq(config.userId, userId))
      .and(QueryBuilder.eq(config.courseId, programId))
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

  def readFromCache(key: String, metrics: Metrics): List[String] = {
    metrics.incCounter(config.cacheHitCount)
    val list = cache.getKeyMembers(key)
    if (CollectionUtils.isEmpty(list)) {
      metrics.incCounter(config.cacheMissCount)
      logger.info("Redis cache (smembers) not available for key: " + key)
    }
    list.asScala.toList
  }

  def updateEnrolContentConsumption(userConsumption: UserContentConsumption)(implicit metrics: Metrics): UserContentConsumption = {
    val programEnrollmentStatus = getEnrolment(userConsumption.userId, userConsumption.courseId)(metrics)
    if (programEnrollmentStatus != null && programEnrollmentStatus.getInt("status") != 2) {
      val programContentStatusList = ListBuffer[Map[String, AnyRef]]()
      val programContentStatus = Option(programEnrollmentStatus.getMap(
        config.contentStatus, TypeToken.of(classOf[String]), TypeToken.of(classOf[Integer]))).head
      for ((key, value) <- userConsumption.contents) {
        // Check if the key is present in leafNodeMap
        if (programContentStatus.get(key) != null) {
          if (value.status == 2 && programContentStatus.get(key) != 2) {
            // Update progress in contentStatus for the matching key
            programContentStatus.put(key, value.status)
          }
        } else {
          programContentStatus.put(key, value.status)
        }
      }

      for( (key, value) <- programContentStatus.asScala) {
        val contentStatusMap = Map(
          config.contentId -> key,
          config.status -> value
        )
        programContentStatusList += contentStatusMap
      }
      // Add programContentStatusMap to programContentStatusList
      val updatedContent = getContentStatusFromEvent(programContentStatusList.toList)
      val updatedUserConsumption = userConsumption.copy(contents = updatedContent)
      updatedUserConsumption
    } else {
      null
    }
  }

  def programEnrolConsumption(userConsumption: UserContentConsumption)(implicit metrics: Metrics): Option[CollectionProgress] = {
    val courseId = userConsumption.courseId
    val userId = userConsumption.userId
    val contextId = "cb:" + userConsumption.batchId
    val key = s"$courseId:$courseId:${config.leafNodes}"
    val leafNodes = readFromCache(key, metrics).distinct
    if (leafNodes.isEmpty) {
      logger.error(s"leaf nodes are not available for: $key")
      //context.output(config.failedEventOutputTag, gson.toJson(userConsumption))
      val status = getCollectionStatus(courseId)
      if (StringUtils.equals("Retired", status)) {
        metrics.incCounter(config.retiredCCEventsCount)
        println(s"contents consumed from a retired collection: $courseId")
        logger.warn(s"contents consumed from a retired collection: $courseId")
        None
      } else {
        metrics.incCounter(config.failedEventCount)
        val message = s"leaf nodes are not available for a published collection: $courseId"
        logger.error(message)
        throw new Exception(message)
      }
    } else {
      val completedCount = leafNodes.intersect(userConsumption.contents.filter(cc => cc._2.status == 2).map(cc => cc._2.contentId).toList.distinct).size
      val contentStatus = userConsumption.contents.map(cc => (cc._2.contentId, cc._2.status)).toMap
      val inputContents = userConsumption.contents.filter(cc => cc._2.fromInput).keys.toList
      val collectionProgress = if (completedCount >= leafNodes.size) {
        Option(CollectionProgress(userId, userConsumption.batchId, courseId, completedCount, new java.util.Date(), contentStatus, inputContents, true))
      } else {
        Option(CollectionProgress(userId, userConsumption.batchId, courseId, completedCount, null, contentStatus, inputContents))
      }
      return collectionProgress
    }
  }

  def getCollectionStatus(collectionId: String): String = {
    val cacheStatus = collectionStatusCache.getNonExpired(collectionId).getOrElse("")
    if (StringUtils.isEmpty(cacheStatus)) {
      val dbStatus = getDBStatus(collectionId)
      collectionStatusCache = collectionStatusCache.putClocked(collectionId, dbStatus)._2
      dbStatus
    } else cacheStatus
  }

  def getDBStatus(collectionId: String): String = {
    val requestBody =
      s"""{
         |    "request": {
         |        "filters": {
         |            "objectType": "Collection",
         |            "identifier": "$collectionId",
         |            "status": ["Live", "Unlisted", "Retired"]
         |        },
         |        "fields": ["status"]
         |    }
         |}""".stripMargin

    val response = httpUtil.post(config.searchAPIURL, requestBody)
    if (response.status == 200) {
      val responseBody = gson.fromJson(response.body, classOf[java.util.Map[String, AnyRef]])
      val result = responseBody.getOrDefault("result", new java.util.HashMap[String, AnyRef]()).asInstanceOf[java.util.Map[String, AnyRef]]
      val count = result.getOrDefault("count", 0.asInstanceOf[Number]).asInstanceOf[Number].intValue()
      if (count > 0) {
        val list = result.getOrDefault("content", new java.util.ArrayList[java.util.Map[String, AnyRef]]()).asInstanceOf[java.util.List[java.util.Map[String, AnyRef]]]
        list.asScala.head.get("status").asInstanceOf[String]
      } else throw new Exception(s"There are no published or retired collection with id: $collectionId")
    } else {
      logger.error("search-service error: " + response.body)
      throw new Exception("search-service not returning error:" + response.status)
    }
  }
}

