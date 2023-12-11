package org.sunbird.job.user.pii.updater.task

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.user.pii.updater.function.{UserPiiEventRouter, UserPiiUpdateFunction}
import org.sunbird.job.user.pii.updater.domain.{Event, UserPiiEvent}
import org.sunbird.job.util.{FlinkUtil, HttpUtil}

import java.io.File
import java.util

class UserPiiUpdaterStreamTask(config: UserPiiUpdaterConfig, kafkaConnector: FlinkKafkaConnector, httpUtil: HttpUtil) {

  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
    implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
    implicit val userPiiTypeInfo: TypeInformation[UserPiiEvent] = TypeExtractor.getForClass(classOf[UserPiiEvent])

    val source = kafkaConnector.kafkaJobRequestSource[Event](config.kafkaInputTopic)
    val processStreamTask = env.addSource(source).name(config.inputConsumerName)
      .uid(config.inputConsumerName).setParallelism(config.kafkaConsumerParallelism)
      .rebalance
      .process(new UserPiiEventRouter(config))
      .name("user-pii-event-router").uid("user-pii-event-router")
      .setParallelism(config.eventRouterParallelism)

    val userPiiStream = processStreamTask.getSideOutput(config.userPiiEventOutTag).process(new UserPiiUpdateFunction(config, httpUtil))
      .name("user-pii-data-updater").uid("user-pii-data-updater").setParallelism(config.userPiiDataUpdaterParallelism)

    env.execute(config.jobName)
  }
}

object UserPiiUpdaterStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("user-pii-data-updater.conf").withFallback(ConfigFactory.systemEnvironment()))
    val userPiiConfig = new UserPiiUpdaterConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(userPiiConfig)
    val httpUtil = new HttpUtil
    val task = new UserPiiUpdaterStreamTask(userPiiConfig, kafkaUtil, httpUtil)
    task.process()
  }
}
