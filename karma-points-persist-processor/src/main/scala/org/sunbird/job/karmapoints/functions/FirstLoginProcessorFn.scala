package org.sunbird.job.karmapoints.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.sunbird.job.karmapoints.domain.Event
import org.sunbird.job.karmapoints.task.KarmaPointsProcessorConfig
import org.sunbird.job.karmapoints.util.Utility._
import org.sunbird.job.util.{CassandraUtil, HttpUtil}
import org.sunbird.job.{BaseProcessFunction, BaseProcessKeyedFunction, Metrics}

class FirstLoginProcessorFn(config: KarmaPointsProcessorConfig, httpUtil: HttpUtil)
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
    val usrId: String = eData.get(config.ID) match {
      case Some(value) => value.asInstanceOf[String]
      case None => ""
    }
    val selfRegistration: Boolean = eData.get(config.SELF_REGISTRATION) match {
      case Some(value) => value.asInstanceOf[Boolean]
      case None => java.lang.Boolean.FALSE
    }

    if(!selfRegistration||doesEntryExist(usrId,config.OPERATION_TYPE_FIRST_LOGIN,config.OPERATION_TYPE_FIRST_LOGIN,usrId)( metrics,config, cassandraUtil))
      return
     kpOnFirstLogin(usrId,config.OPERATION_TYPE_FIRST_LOGIN,config.OPERATION_TYPE_FIRST_LOGIN,usrId,config, cassandraUtil)(metrics)
  }
  private def kpOnFirstLogin(userId : String, contextType : String, operationType:String, contextId:String, config: KarmaPointsProcessorConfig, cassandraUtil: CassandraUtil)(metrics: Metrics) :Unit = {
    val points: Int = config.firstLoginQuotaKarmaPoints
    insertKarmaPoints(userId, contextType,operationType,contextId,points,config,cassandraUtil)(metrics)
    updateKarmaSummary(userId, points) ( config, cassandraUtil)
  }
}