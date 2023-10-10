package org.sunbird.job.programaggregate.functions

import java.lang.reflect.Type
import java.util.concurrent.TimeUnit
import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.{QueryBuilder, Select, Update}
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
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
import org.sunbird.job.util.{CassandraUtil, HttpUtil, ScalaJsonUtil}
import org.sunbird.job.{Metrics, WindowBaseProcessFunction}

import scala.collection.JavaConverters._
import scala.collection.mutable

class ProgramActivityAggregatesFunction(config: ProgramActivityAggregateUpdaterConfig, httpUtil: HttpUtil, @transient var cassandraUtil: CassandraUtil = null)
                                       (implicit val stringTypeInfo: TypeInformation[String])
  extends WindowBaseProcessFunction[Map[String, AnyRef], String, Int](config) {

  val mapType: Type = new TypeToken[Map[String, AnyRef]]() {}.getType
  private[this] val logger = LoggerFactory.getLogger(classOf[ProgramActivityAggregatesFunction])
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

    logger.debug("Input Events Size: " + events.toList.size)

    var updatedEventInfo: mutable.Buffer[Map[String, AnyRef]] = mutable.Buffer.empty
    events.map(value => {
      var eventInfoMap: mutable.Iterable[Map[String, AnyRef]] = getProgramEvent(value)(metrics, config, httpUtil, cache)
      if (eventInfoMap != null) {
        updatedEventInfo ++= eventInfoMap
        updatedEventInfo
      }
    })
    val inputUserConsumptionList: List[UserContentConsumption] = updatedEventInfo
      .groupBy(key => (key.get(config.courseId), key.get(config.batchId), key.get(config.userId)))
      .values.map(value => {
        metrics.incCounter(config.processedEnrolmentCount)
        val batchId = value.head(config.batchId).toString
        val userId = value.head(config.userId).toString
        val courseId = value.head(config.courseId).toString
        val userConsumedContents = value.head(config.contents).asInstanceOf[List[Map[String, AnyRef]]]
        val enrichedContents = getContentStatusFromEvent(userConsumedContents)
        UserContentConsumption(userId = userId, batchId = batchId, courseId = courseId, enrichedContents)
      }).toList

    if (inputUserConsumptionList.isEmpty)
      return
    // Fetch the content status from the table in batch format
    val dbUserConsumption: Map[String, UserContentConsumption] = getContentStatusFromDB(events.toList, metrics)

    // Final User's ContentConsumption after merging with DB data.
    // Here we have final viewcount, completedcount and identified the content which should generate AUDIT events for start and complete.
    val finalUserConsumptionList = inputUserConsumptionList.map(inputData => {
      val dbData = dbUserConsumption.getOrElse(getUCKey(inputData), UserContentConsumption(inputData.userId, inputData.batchId, inputData.courseId, Map()))
      finalUserConsumption(inputData, dbData)(metrics)
    })

    // user_content_consumption update with viewcount and completedcout.
    val userConsumptionQueries = finalUserConsumptionList.flatMap(userConsumption => getContentConsumptionQueries(userConsumption))
    updateDB(config.thresholdBatchWriteSize, userConsumptionQueries)(metrics)


    val courseAggregations = finalUserConsumptionList.flatMap(userConsumption => {

      // Course Level Agg using the merged data of ContentConsumption per user, course and batch.
      val optCourseAgg = courseActivityAgg(userConsumption, context)(metrics)
      val courseAggs = if (optCourseAgg.nonEmpty) List(optCourseAgg.get) else List()

      // Identify the children of the course (only collections) for which aggregates computation required.
      // Computation of aggregates using leafNodes (of the specific collection) and user completed contents.
      // Here computing only "completedCount" aggregate.
      if (config.moduleAggEnabled) {
        val courseChildrenAggs = courseChildrenActivityAgg(userConsumption)(metrics)
        courseAggs ++ courseChildrenAggs
      } else courseAggs
    })

    // Saving all queries for course and it's children (only collection) aggregates.
    val aggQueries = courseAggregations.map(agg => getUserAggQuery(agg.activityAgg))
    updateDB(config.thresholdBatchWriteSize, aggQueries)(metrics)

    // Saving enrolment completion data.
    val collectionProgressList = courseAggregations.filter(agg => agg.collectionProgress.nonEmpty).map(agg => agg.collectionProgress.get)

    val collectionProgressUpdateList = collectionProgressList.filter(progress => !progress.completed)
    context.output(config.collectionUpdateOutputTag, collectionProgressUpdateList)

    val collectionProgressCompleteList = collectionProgressList.filter(progress => progress.completed)
    context.output(config.collectionCompleteOutputTag, collectionProgressCompleteList)

    // Content AUDIT Event generation and pushing to output tag.
    finalUserConsumptionList.flatMap(userConsumption => contentAuditEvents(userConsumption)).foreach(event => context.output(config.auditEventOutputTag, gson.toJson(event)))
  }

  /**
   * Course Level Agg using the merged data of ContentConsumption per user, course and batch.
   */
  def courseActivityAgg(userConsumption: UserContentConsumption, context: ProcessWindowFunction[Map[String, AnyRef], String, Int, GlobalWindow]#Context)(implicit metrics: Metrics): Option[UserEnrolmentAgg] = {
    val courseId = userConsumption.courseId
    val userId = userConsumption.userId
    val contextId = "cb:" + userConsumption.batchId
    val key = s"$courseId:$courseId:${config.leafNodes}"
    val leafNodes = readFromCache(key, metrics).distinct
    if (leafNodes.isEmpty) {
      logger.error(s"leaf nodes are not available for: $key")
      context.output(config.failedEventOutputTag, gson.toJson(userConsumption))
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
      Option(UserEnrolmentAgg(UserActivityAgg("Course", userId, courseId, contextId, Map("completedCount" -> completedCount.toDouble), Map("completedCount" -> System.currentTimeMillis())), collectionProgress))
    }
  }

  /**
   * Identified the children of the course (only collections) for which aggregates computation required.
   * Computation of aggregates using leafNodes (of the specific collection) and user completed contents.
   * Here computing only "completedCount" aggregate.
   */
  def courseChildrenActivityAgg(userConsumption: UserContentConsumption)(implicit metrics: Metrics): List[UserEnrolmentAgg] = {
    val courseId = userConsumption.courseId
    val userId = userConsumption.userId
    val contextId = "cb:" + userConsumption.batchId

    // These are the child collections which require computation of aggregates - for this user.
    val ancestors = userConsumption.contents.mapValues(content => {
      val contentId = content.contentId
      readFromCache(key = s"$courseId:$contentId:${config.ancestors}", metrics)
    }).values.flatten.filter(a => !StringUtils.equals(a, courseId)).toList.distinct

    // LeafNodes of the identified child collections - for this user.
    val collectionsWithLeafNodes = ancestors.map(unitId => {
      (unitId, readFromCache(key = s"$courseId:$unitId:${config.leafNodes}", metrics).distinct)
    }).toMap

    // Content completed - By this user.
    val userCompletedContents = userConsumption.contents.filter(cc => cc._2.status == 2).map(cc => cc._2.contentId).toList.distinct

    // Child Collection UserAggregate list - for this user.
    collectionsWithLeafNodes.map(e => {
      val collectionId = e._1
      val leafNodes = e._2
      val completedCount = leafNodes.intersect(userCompletedContents).size
      /* TODO - List
       TODO 1. Generalise activityType from "Course" to "Collection".
       TODO 2.Identify how to generate start and end event for CourseUnit.
       */
      val activityAgg = UserActivityAgg("Course", userId, collectionId, contextId, Map("completedCount" -> completedCount), Map("completedCount" -> System.currentTimeMillis()))
      UserEnrolmentAgg(activityAgg, None)
    }).toList
  }

  /**
   * Generation of a "String" key for UserContentConsumption.
   */
  def getUCKey(userConsumption: UserContentConsumption): String = {
    userConsumption.userId + ":" + userConsumption.courseId + ":" + userConsumption.batchId
  }

  /**
   * Merging the Input and DB ContentStatus data of a User, Course and Batch (Enrolment)
   * This is the critical part of the code.
   */
  def finalUserConsumption(inputData: UserContentConsumption, dbData: UserContentConsumption)(implicit metrics: Metrics): UserContentConsumption = {
    val dbContents = dbData.contents
    val processedContents = inputData.contents.map {
      case (contentId, inputCC) => {
        // ContentStatus from DB.
        val dbCC: ContentStatus = dbContents.getOrElse(contentId, ContentStatus(contentId, 0, 0, 0))
        val finalStatus = List(inputCC.status, dbCC.status).max // Final status is max of DB and Input ContentStatus.
        val views = sumFunc(List(inputCC, dbCC), (x: ContentStatus) => {
          x.viewCount
        }) // View Count is sum of DB and Input ContentStatus.
        val completion = sumFunc(List(inputCC, dbCC), (x: ContentStatus) => {
          x.completedCount
        }) // Completed Count is sum of DB and Input ContentStatus.
        val eventsFor: List[String] = getEventActions(dbCC, inputCC)
        // Merged ContentStatus.
        (contentId, ContentStatus(contentId, finalStatus, completion, views, inputCC.fromInput, eventsFor))
      }
    }

    val existingContents = processedContents.keys.toList
    val remainingContents = dbData.contents.filterKeys(key => !existingContents.contains(key))
    val finalContentsMap = processedContents ++ remainingContents
    UserContentConsumption(inputData.userId, inputData.batchId, inputData.courseId, finalContentsMap)
  }

  /**
   * This will identify whether this is the start or complete of the Content by User.
   *
   * @return List - Actions - "start" and "complete".
   */
  def getEventActions(dbCC: ContentStatus, inputCC: ContentStatus): List[String] = {
    val startAction = if (dbCC.viewCount == 0) List("start") else List()
    val completeAction = if (dbCC.completedCount == 0 && inputCC.completedCount > 0) List(config.complete) else List()
    startAction ::: completeAction
  }

  /**
   * Generic method to read data from DB (Cassandra).
   *
   * @return
   */
  def readFromDB(columns: Map[String, AnyRef], keySpace: String, table: String, metrics: Metrics): List[Row] = {
    val selectWhere: Select.Where = QueryBuilder.select().all()
      .from(keySpace, table).
      where()
    columns.map(col => {
      col._2 match {
        case value: List[Any] =>
          selectWhere.and(QueryBuilder.in(col._1, value.asJava))
        case _ =>
          selectWhere.and(QueryBuilder.eq(col._1, col._2))
      }
    })
    metrics.incCounter(config.dbReadCount)
    cassandraUtil.find(selectWhere.toString).asScala.toList

  }

  /**
   * Method to update the specific table in a batch format.
   */
  def updateDB(batchSize: Int, queriesList: List[Update.Where])(implicit metrics: Metrics): Unit = {
    val groupedQueries = queriesList.grouped(batchSize).toList
    groupedQueries.foreach(queries => {
      val cqlBatch = QueryBuilder.batch()
      queries.map(query => cqlBatch.add(query))
      val result = cassandraUtil.upsert(cqlBatch.toString)
      if (result) {
        metrics.incCounter(config.dbUpdateCount)
      } else {
        val msg = "Database update has failed: " + cqlBatch.toString
        logger.error(msg)
        throw new Exception(msg)
      }
    })
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

  def getUserAggQuery(progress: UserActivityAgg):
  Update.Where = {
    QueryBuilder.update(config.dbKeyspace, config.dbUserActivityAggTable)
      .`with`(QueryBuilder.putAll(config.aggregates, progress.aggregates.asJava))
      .and(QueryBuilder.putAll(config.aggLastUpdated, progress.agg_last_updated.asJava))
      .where(QueryBuilder.eq(config.activityId, progress.activity_id))
      .and(QueryBuilder.eq(config.activityType, progress.activity_type))
      .and(QueryBuilder.eq(config.contextId, progress.context_id))
      .and(QueryBuilder.eq(config.activityUser, progress.user_id))
  }

  /**
   * Creates the cql query for content consumption table
   */
  def getContentConsumptionQueries(userContentConsumption: UserContentConsumption): List[Update.Where] = {
    userContentConsumption.contents.mapValues(content => {
      QueryBuilder.update(config.dbKeyspace, config.dbUserContentConsumptionTable)
        .`with`(QueryBuilder.set(config.viewcount, content.viewCount))
        .and(QueryBuilder.set(config.completedcount, content.completedCount))
        .where(QueryBuilder.eq(config.batchId.toLowerCase(), userContentConsumption.batchId))
        .and(QueryBuilder.eq(config.courseId.toLowerCase(), userContentConsumption.courseId))
        .and(QueryBuilder.eq(config.userId.toLowerCase(), userContentConsumption.userId))
        .and(QueryBuilder.eq(config.contentId.toLowerCase(), content.contentId))
    }).values.toList
  }

  /**
   * Method to get the content status object in map format ex: (do_5874308329084 -> 2, do_59485345435 -> 3)
   * It always takes the highest precedence progress values for the contents ex: (do_5874308329084 -> 2, do_5874308329084 -> 1, do_59485345435 -> 3) => (do_5874308329084 -> 2, do_59485345435 -> 3)
   *
   * Ex: Map("C1"->2, "C2" ->1)
   *
   */
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


  /**
   * Method to get the content status from the database
   *
   * Ex: List(Map("courseId" -> "do_43795", batchId -> "batch1", userId->"user001", contentStatus -> Map("C1"->2, "C2" ->1)))
   *
   */
  def getContentStatusFromDB(eDataBatch: List[Map[String, AnyRef]], metrics: Metrics): Map[String, UserContentConsumption] = {

    val contentConsumption = scala.collection.mutable.Map[String, UserContentConsumption]()
    val primaryFields = Map(
      config.userId.toLowerCase() -> eDataBatch.map(x => x(config.userId)).distinct,
      config.batchId.toLowerCase -> eDataBatch.map(x => x(config.batchId)).distinct,
      config.courseId.toLowerCase -> eDataBatch.map(x => x(config.courseId)).distinct
    )

    val records = Option(readFromDB(primaryFields, config.dbKeyspace, config.dbUserContentConsumptionTable, metrics))
    records.map(record => record.groupBy(col => Map(config.batchId -> col.getObject(config.batchId.toLowerCase()).asInstanceOf[String], config.userId -> col.getObject(config.userId.toLowerCase()).asInstanceOf[String], config.courseId -> col.getObject(config.courseId.toLowerCase()).asInstanceOf[String])))
      .foreach(groupedRecords => groupedRecords.map(entry => {
        val identifierMap = entry._1
        val consumptionList = entry._2.flatMap(row => Map(row.getObject(config.contentId.toLowerCase()).asInstanceOf[String] -> Map(config.status -> row.getObject(config.status), config.viewcount -> row.getObject(config.viewcount), config.completedcount -> row.getObject(config.completedcount))))
          .map(entry => {
            val contentStatus = entry._2.filter(x => x._2 != null)
            val contentId = entry._1
            val status = contentStatus.getOrElse(config.status, 1).asInstanceOf[Number].intValue()
            val viewCount = contentStatus.getOrElse(config.viewcount, 0).asInstanceOf[Number].intValue()
            val completedCount = contentStatus.getOrElse(config.completedcount, 0).asInstanceOf[Number].intValue()
            (contentId, ContentStatus(contentId, status, completedCount, viewCount, false))
          }).toMap

        val userId = identifierMap(config.userId)
        val batchId = identifierMap(config.batchId)
        val courseId = identifierMap(config.courseId)

        val userContentConsumption = UserContentConsumption(userId, batchId, courseId, consumptionList)
        contentConsumption += getUCKey(userContentConsumption) -> userContentConsumption

      }))
    contentConsumption.toMap
  }

  /**
   * Content - AUDIT Event Generation using UserContentConsumption
   * "eventsFor" - will have the action (or type) for the event to generate.
   */
  def contentAuditEvents(userConsumption: UserContentConsumption): List[TelemetryEvent] = {
    val userId = userConsumption.userId
    val courseId = userConsumption.courseId
    val batchId = userConsumption.batchId
    val contentsForEvents = userConsumption.contents.filter(c => c._2.eventsFor.nonEmpty).values
    contentsForEvents.flatMap(c => {
      c.eventsFor.map(action => {
        val properties = if (StringUtils.equalsIgnoreCase(action, config.complete)) Array(config.viewcount, config.completedcount) else Array(config.viewcount)
        TelemetryEvent(
          actor = ActorObject(id = userId),
          edata = EventData(props = properties, `type` = action), // action values are "start", "complete".
          context = EventContext(cdata = Array(Map("type" -> config.courseBatch, "id" -> batchId).asJava)),
          `object` = EventObject(id = c.contentId, `type` = "Content", rollup = Map[String, String]("l1" -> courseId).asJava)
        )
      })
    }).toList
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

  def getCollectionStatus(collectionId: String): String = {
    val cacheStatus = collectionStatusCache.getNonExpired(collectionId).getOrElse("")
    if (StringUtils.isEmpty(cacheStatus)) {
      val dbStatus = getDBStatus(collectionId)
      collectionStatusCache = collectionStatusCache.putClocked(collectionId, dbStatus)._2
      dbStatus
    } else cacheStatus
  }

  def verifyPrimaryCategory(identifier: String)(
    metrics: Metrics,
    config: ProgramActivityAggregateUpdaterConfig,
    httpUtil: HttpUtil,
    cache: DataCache
  ): Boolean = {
    logger.info(
      "Verify Program post-publish required for content: " + identifier
    )
    // Get the primary Categories for the courses here
    var isValidProgram = false
    val contentObj: java.util.Map[String, AnyRef] =
      getCourseInfo(identifier)(metrics, config, cache, httpUtil)
    if (!contentObj.isEmpty) {
      val primaryCategory = contentObj.get("primaryCategory")
      if (primaryCategory != null &&
        (primaryCategory == "Program"
          || primaryCategory == "Curated Program"
          || primaryCategory == "Blended Program")) {
        isValidProgram = true
      }
      logger.info("PrimaryCategory value is :" + primaryCategory + ", for Id: " + identifier)
    } else {
      logger.error("Failed to read content details for Id: " + identifier)
    }
    logger.info("is program activity aggregator is skipping this event ? " + isValidProgram)
    isValidProgram
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
        config.contentReadURL + courseId + "?fields=identifier,name,primaryCategory"
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
      val courseInfoMap: java.util.Map[String, AnyRef] =
        new java.util.HashMap[String, AnyRef]()
      courseInfoMap.put("courseId", courseId)
      courseInfoMap.put("courseName", courseName)
      courseInfoMap.put("primaryCategory", primaryCategory)
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
      val courseInfoMap: java.util.Map[String, AnyRef] =
        new java.util.HashMap[String, AnyRef]()
      courseInfoMap.put("courseId", courseId)
      courseInfoMap.put("courseName", courseName)
      courseInfoMap.put("primaryCategory", primaryCategory)
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

  def getProgramEvent(eventData: Map[String, AnyRef])(
    metrics: Metrics,
    config: ProgramActivityAggregateUpdaterConfig,
    httpUtil: HttpUtil,
    cache: DataCache
  ): mutable.Iterable[Map[String, AnyRef]] = {
    var eventInfoMap: mutable.ListBuffer[Map[String, AnyRef]] = mutable.ListBuffer.empty[Map[String, AnyRef]]
    val userId: String = eventData.getOrElse(config.userId, "").asInstanceOf[String]
    val courseId: String = eventData.getOrElse(config.courseId, "").asInstanceOf[String]
    val batchId: String = eventData.getOrElse(config.batchId, "").asInstanceOf[String]
    val primaryCategory: String = eventData.getOrElse(config.primaryCategory, "").asInstanceOf[String]
    val parentCollections: List[String] = eventData.getOrElse(config.parentCollections, List.empty[String]).asInstanceOf[List[String]]
    if (config.validProgramPrimaryCategory.contains(primaryCategory)) {
      eventInfoMap += eventData
      var eventInfo : Map[String, AnyRef] = Map.empty
      eventInfo ++= eventData
      if (StringUtils.isEmpty(batchId)) {
        val row = getEnrolment(userId, courseId)(metrics)
        if (row != null) {
          eventInfo += ("batchId" -> row.getString("batchid"))
        } else {
          return null;
        }
        eventInfoMap += eventInfo
      }
    } else if (StringUtils.isNotBlank(primaryCategory) && (primaryCategory.equals("Course") || primaryCategory.equals("Standalone Assessment"))
      && !parentCollections.isEmpty) {
      eventInfoMap += eventData
      for (parentId <- parentCollections) {
        val row = getEnrolment(userId, parentId)(metrics)
        if (row != null) {
          val contentConsumption: List[String] = eventData.getOrElse(config.contents, List.empty[String]).asInstanceOf[List[String]]
          val eventInfoProgram = Map[String, AnyRef]("edata" ->
            Map("contents" -> contentConsumption,
              "userId" -> "${userId}",
              "action" -> "batch-enrolment-update",
              "iteration" -> 1, "batchId" -> row.getString("batchid"),
              "courseId" -> parentId))
          eventInfoMap += eventInfoProgram
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
}

