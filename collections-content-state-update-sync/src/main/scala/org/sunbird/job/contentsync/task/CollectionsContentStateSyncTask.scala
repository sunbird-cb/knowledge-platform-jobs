package org.sunbird.job.contentsync.task

import java.io.File
import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.slf4j.LoggerFactory
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.contentsync.domain.Event
import org.sunbird.job.contentsync.functions.CollectionsContentStateSyncFn
import org.sunbird.job.util.{FlinkUtil, HttpUtil}

class CollectionsContentStateSyncTask(config: CollectionsContentStateSyncConfig, kafkaConnector: FlinkKafkaConnector, httpUtil: HttpUtil) {
    private[this] val logger = LoggerFactory.getLogger(classOf[CollectionsContentStateSyncTask])

    def process(): Unit = {
        implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
        implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
        implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
        val source = kafkaConnector.kafkaJobRequestSource[Event](config.kafkaInputTopic)
        logger.info("This is under process for task")
        val progressStream =
            env.addSource(source).name(config.certificatePreProcessorConsumer)
              .uid(config.certificatePreProcessorConsumer).setParallelism(config.kafkaConsumerParallelism)
              .rebalance
              .keyBy(new CollectionsContentStateSyncKeySelector())
              .process(new CollectionsContentStateSyncFn(config, httpUtil))
              .name("collections-content-state-sync").uid("collections-content-state-sync")
              .setParallelism(config.parallelism)

        progressStream.getSideOutput(config.generateCertificateOutputTag).addSink(kafkaConnector.kafkaStringSink(config.kafkaOutputTopic))
          .name(config.generateCertificateProducer).uid(config.generateCertificateProducer).setParallelism(config.generateCertificateParallelism)
        env.execute(config.jobName)
    }
}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster

object CollectionsContentStateSyncTask {
    def main(args: Array[String]): Unit = {
        val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
        val config = configFilePath.map {
            path => ConfigFactory.parseFile(new File(path)).resolve()
        }.getOrElse(ConfigFactory.load("collections-content-state-update-sync.conf").withFallback(ConfigFactory.systemEnvironment()))
        val certificatePreProcessorConfig = new CollectionsContentStateSyncConfig(config)
        val kafkaUtil = new FlinkKafkaConnector(certificatePreProcessorConfig)
        val httpUtil = new HttpUtil()
        val task = new CollectionsContentStateSyncTask(certificatePreProcessorConfig, kafkaUtil, httpUtil)
        task.process()
    }
}

class CollectionsContentStateSyncKeySelector extends KeySelector[Event, String] {
    override def getKey(event: Event): String = Set(event.userId, event.courseId, event.batchId).mkString("_")
}