package org.sunbird.job.karmapoints.task

import java.io.File
import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.slf4j.LoggerFactory
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.karmapoints.domain.Event
import org.sunbird.job.karmapoints.functions.KarmaPointsProcessorFn
import org.sunbird.job.util.{FlinkUtil, HttpUtil}

class KarmaPointsProcessorTask(config: KarmaPointsProcessorConfig, kafkaConnector: FlinkKafkaConnector, httpUtil: HttpUtil) {
    private[this] val logger = LoggerFactory.getLogger(classOf[KarmaPointsProcessorTask])

    def process(): Unit = {
        implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
        //implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment()
        implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
        implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
        val source = kafkaConnector.kafkaJobRequestSource[Event](config.kafkaInputTopic)
        logger.info("This is under process for task")
        val progressStream =
            env.addSource(source).name(config.karmaPointsPersistProcessorConsumer)
              .uid(config.karmaPointsPersistProcessorConsumer).setParallelism(config.kafkaConsumerParallelism)
              .rebalance
              .keyBy(new KarmaPointsProcessorKeySelector())
              .process(new KarmaPointsProcessorFn(config, httpUtil))
              .name("karma-points-processor").uid("karma-points-processor")
              .setParallelism(config.parallelism)
            env.execute(config.jobName)
    }
}


object KarmaPointsProcessorTask {
    def main(args: Array[String]): Unit = {
        val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
        val config = configFilePath.map {
            path => ConfigFactory.parseFile(new File(path)).resolve()
        }.getOrElse(ConfigFactory.load("Karma-points-processor.conf").withFallback(ConfigFactory.systemEnvironment()))
        val karmaPointsProcessorConfig = new KarmaPointsProcessorConfig(config)
        val kafkaUtil = new FlinkKafkaConnector(karmaPointsProcessorConfig)
        val httpUtil = new HttpUtil()
        val task = new KarmaPointsProcessorTask(karmaPointsProcessorConfig, kafkaUtil, httpUtil)
        task.process()
    }
}

class KarmaPointsProcessorKeySelector extends KeySelector[Event, String] {
    override def getKey(event: Event): String = Set(event.userId, event.contextId, event.contextType, event.operationType).mkString("_")
}