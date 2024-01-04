package org.sunbird.job.karmapoints.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.sunbird.job.karmapoints.domain.Event
import org.sunbird.job.karmapoints.task.KarmaPointsProcessorConfig
import org.sunbird.job.karmapoints.util.Utility
import org.sunbird.job.util.{CassandraUtil, HttpUtil}
import org.sunbird.job.{BaseProcessFunction, BaseProcessKeyedFunction, Metrics}

class KarmaPointsFirstLoginProcessorFn(config: KarmaPointsProcessorConfig, httpUtil: HttpUtil)
                                      (implicit val stringTypeInfo: TypeInformation[String],
                                @transient var cassandraUtil: CassandraUtil = null)
  extends BaseProcessFunction[Event, String](config) {

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
  }

  override def close(): Unit = {
    cassandraUtil.close()
    super.close()
  }

  override def metricsList(): List[String] = {
    List(config.totalEventsCount, config.dbReadCount, config.dbUpdateCount, config.failedEventCount, config.skippedEventCount, config.successEventCount,
      config.cacheHitCount, config.karmaPointsIssueEventsCount, config.cacheMissCount)
  }

  override def processElement(event: Event,
                              context: ProcessFunction[Event, String]#Context,
                              metrics: Metrics): Unit = {
    val eData = event.getMap().get(config.EDATA).asInstanceOf[scala.collection.immutable.Map[String, Any]]
    val userIdSome : Option[Any] = eData.get(config.ID)
    val usrId: String = userIdSome match {
      case Some(value) => value.asInstanceOf[String]
      case None => ""
    }
    if(Utility.isEntryAlreadyExist(usrId,config.OPERATION_TYPE_FIRST_LOGIN,config.OPERATION_TYPE_FIRST_LOGIN,usrId,config,cassandraUtil))
      return
     firstLogin(usrId,config.OPERATION_TYPE_FIRST_LOGIN,config.OPERATION_TYPE_FIRST_LOGIN,usrId,config, cassandraUtil)(metrics)
  }
  private def firstLogin(userId : String, contextType : String,operationType:String,contextId:String,config: KarmaPointsProcessorConfig, cassandraUtil: CassandraUtil)(metrics: Metrics) :Unit = {
    val points: Int = config.firstLoginQuotaKarmaPoints
    Utility.insertKarmaPoints(userId, contextType,operationType,contextId,points,config,cassandraUtil)(metrics)
    Utility.updateKarmaSummary(userId, points, config, cassandraUtil)
  }
}