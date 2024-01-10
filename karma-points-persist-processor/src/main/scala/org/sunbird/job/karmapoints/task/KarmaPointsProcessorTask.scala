package org.sunbird.job.karmapoints.task
import java.io.File
import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.slf4j.LoggerFactory
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.karmapoints.domain.Event
import org.sunbird.job.karmapoints.functions.{ClaimACBPProcessorFn, CourseCompletionProcessorFn, FirstEnrolmentProcessorFn, FirstLoginProcessorFn, RatingProcessorFn}
import org.sunbird.job.util.{FlinkUtil, HttpUtil}

class KarmaPointsProcessorTask(config: KarmaPointsProcessorConfig, kafkaConnector: FlinkKafkaConnector, httpUtil: HttpUtil) {
  private[this] val logger = LoggerFactory.getLogger(classOf[KarmaPointsProcessorTask])

  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
   // implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment()
    implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

    env.addSource(kafkaConnector.kafkaJobRequestSource[Event](config.kafkaInputCourseCompletionTopic))
      .name(config.karmaPointsCourseCompletionPersistProcessorConsumer)
      .uid(config.karmaPointsCourseCompletionPersistProcessorConsumer)
      .setParallelism(config.kafkaConsumerParallelism)
      .rebalance
      .process(new CourseCompletionProcessorFn(config, httpUtil))
      .setParallelism(config.parallelism)

    env.addSource(kafkaConnector.kafkaJobRequestSource[Event](config.kafkaInputRatingTopic))
      .name(config.karmaPointsRatingPersistProcessorConsumer)
      .uid(config.karmaPointsRatingPersistProcessorConsumer)
      .setParallelism(config.kafkaConsumerParallelism)
      .rebalance
      .process(new RatingProcessorFn(config, httpUtil))
      .setParallelism(config.parallelism)

    env.addSource(kafkaConnector.kafkaJobRequestSource[Event](config.kafkaInputFirstEnrolmentTopic))
      .name(config.karmaPointsFirstEnrolmentPersistProcessorConsumer)
      .uid(config.karmaPointsFirstEnrolmentPersistProcessorConsumer)
      .setParallelism(config.kafkaConsumerParallelism)
      .rebalance
      .process(new FirstEnrolmentProcessorFn(config, httpUtil))
      .setParallelism(config.parallelism)

    env.addSource(kafkaConnector.kafkaJobRequestSource[Event](config.kafkaInputFirstLoginTopic))
      .name(config.karmaPointsFirstLoginPersistProcessorConsumer)
      .uid(config.karmaPointsFirstLoginPersistProcessorConsumer)
      .setParallelism(config.kafkaConsumerParallelism)
      .rebalance
      .process(new FirstLoginProcessorFn(config, httpUtil))
      .setParallelism(config.parallelism)

    env.addSource(kafkaConnector.kafkaJobRequestSource[Event](config.kafkaInputClaimACBPTopic))
      .name(config.karmaPointsClaimACBPPersistProcessorConsumer)
      .uid(config.karmaPointsClaimACBPPersistProcessorConsumer)
      .setParallelism(config.kafkaConsumerParallelism)
      .rebalance
      .process(new ClaimACBPProcessorFn(config, httpUtil))
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