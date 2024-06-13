package org.sunbird.job.programaggregate.task

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.sunbird.job.programaggregate.domain.CollectionProgress
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.programaggregate.functions.{ProgramActivityAggregatesEnrolUpdateFunction, ProgramActivityAggregatesFunction, ProgramContentConsumptionDeDupFunction, ProgramProgressCompleteFunction, ProgramProgressUpdateFunction}
import org.sunbird.job.util.{FlinkUtil, HttpUtil}

import java.io.File
import java.util


class ProgramActivityAggregateUpdaterStreamTask(config: ProgramActivityAggregateUpdaterConfig, kafkaConnector: FlinkKafkaConnector, httpUtil: HttpUtil) {
  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
    implicit val enrolmentCompleteTypeInfo: TypeInformation[List[CollectionProgress]] = TypeExtractor.getForClass(classOf[List[CollectionProgress]])

    val progressStream =
      env.addSource(kafkaConnector.kafkaMapSource(config.kafkaInputTopic)).name(config.programActivityAggregateUpdaterConsumer)
        .uid(config.programActivityAggregateUpdaterConsumer).setParallelism(config.kafkaConsumerParallelism)
        .rebalance
        .process(new ProgramContentConsumptionDeDupFunction(config, httpUtil)).name(config.consumptionDeDupFn)
        .uid(config.consumptionDeDupFn).setParallelism(config.deDupProcessParallelism)
        .getSideOutput(config.uniqueConsumptionOutput)
        .keyBy(new ProgramActivityAggregatorKeySelector(config))
        .countWindow(config.thresholdBatchReadSize)
        .process(new ProgramActivityAggregatesEnrolUpdateFunction(config, httpUtil))
        .name(config.programactivityAggregateUpdaterFn)
        .uid(config.programactivityAggregateUpdaterFn)
        .setParallelism(config.activityAggregateUpdaterParallelism)

    progressStream.getSideOutput(config.auditEventOutputTag).addSink(kafkaConnector.kafkaStringSink(config.kafkaAuditEventTopic))
      .name(config.programactivityAggregateUpdaterProducer).uid(config.programactivityAggregateUpdaterProducer)
    progressStream.getSideOutput(config.failedEventOutputTag).addSink(kafkaConnector.kafkaStringSink(config.kafkaFailedEventTopic))
      .name(config.programactivityAggFailedEventProducer).uid(config.programactivityAggFailedEventProducer)

    progressStream.getSideOutput(config.collectionUpdateOutputTag).process(new ProgramProgressUpdateFunction(config))
      .name(config.collectionProgressUpdateFn).uid(config.collectionProgressUpdateFn).setParallelism(config.enrolmentCompleteParallelism)
    val enrolmentCompleteStream = progressStream.getSideOutput(config.collectionCompleteOutputTag).process(new ProgramProgressCompleteFunction(config))
      .name(config.collectionCompleteFn).uid(config.collectionCompleteFn).setParallelism(config.enrolmentCompleteParallelism)

    enrolmentCompleteStream.getSideOutput(config.certIssueOutputTag).addSink(kafkaConnector.kafkaStringSink(config.kafkaCertIssueTopic))
      .name(config.certIssueEventProducer).uid(config.certIssueEventProducer)
    /*enrolmentCompleteStream.getSideOutput(config.auditEventOutputTag).addSink(kafkaConnector.kafkaStringSink(config.kafkaAuditEventTopic))
      .name(config.enrolmentCompleteEventProducer).uid(config.enrolmentCompleteEventProducer)*/

    env.execute(config.jobName)
  }

}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object ProgramActivityAggregateUpdaterStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("program-activity-aggregate-updater.conf").withFallback(ConfigFactory.systemEnvironment()))
    val courseAggregator = new ProgramActivityAggregateUpdaterConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(courseAggregator)
    val httpUtil = new HttpUtil
    val task = new ProgramActivityAggregateUpdaterStreamTask(courseAggregator, kafkaUtil, httpUtil)
    task.process()
  }

}
// $COVERAGE-ON$

class ProgramActivityAggregatorKeySelector(config: ProgramActivityAggregateUpdaterConfig) extends KeySelector[Map[String, AnyRef], Int] {
  private val serialVersionUID = 7267989625042068736L
  private val shards = config.windowShards
  override def getKey(in: Map[String, AnyRef]): Int = {
    in.getOrElse(config.userId, "").asInstanceOf[String].hashCode % shards
  }
}
