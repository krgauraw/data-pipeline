package org.sunbird.job.quml.migrator.task

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.quml.migrator.domain.{Event, MigrationMetadata}
import org.sunbird.job.quml.migrator.function.{QuestionMigrationFunction, QuestionSetMigrationFunction, QumlMigrationEventRouter}
import org.sunbird.job.util.{FlinkUtil, HttpUtil}

import java.io.File
import java.util

class QumlMigratorStreamTask(config: QumlMigratorConfig, kafkaConnector: FlinkKafkaConnector, httpUtil: HttpUtil) {

	def process(): Unit = {
		implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
		implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
		implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
		implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
		implicit val migrationMetaTypeInfo: TypeInformation[MigrationMetadata] = TypeExtractor.getForClass(classOf[MigrationMetadata])

		val source = kafkaConnector.kafkaJobRequestSource[Event](config.kafkaInputTopic)
		val processStreamTask = env.addSource(source).name(config.inputConsumerName)
		  .uid(config.inputConsumerName).setParallelism(config.kafkaConsumerParallelism)
		  .rebalance
		  .process(new QumlMigrationEventRouter(config))
		  .name("migration-event-router").uid("migration-event-router")
		  .setParallelism(config.eventRouterParallelism)

		val questionStream = processStreamTask.getSideOutput(config.questionMigrationOutTag).process(new QuestionMigrationFunction(config, httpUtil))
		  .name("question-migration-process").uid("question-migration-process").setParallelism(1)

		val questionSetStream = processStreamTask.getSideOutput(config.questionSetMigrationOutTag).process(new QuestionSetMigrationFunction(config, httpUtil))
		  .name("questionset-migration-process").uid("questionset-migration-process").setParallelism(1)

		questionStream.getSideOutput(config.liveQuestionPublishEventOutTag).addSink(kafkaConnector.kafkaStringSink(config.republishTopic))
		questionSetStream.getSideOutput(config.liveQuestionSetPublishEventOutTag).addSink(kafkaConnector.kafkaStringSink(config.republishTopic))
		env.execute(config.jobName)
	}
}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object QumlMigratorStreamTask {

	def main(args: Array[String]): Unit = {
		val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
		val config = configFilePath.map {
			path => ConfigFactory.parseFile(new File(path)).resolve()
		}.getOrElse(ConfigFactory.load("quml-migrator.conf").withFallback(ConfigFactory.systemEnvironment()))
		val publishConfig = new QumlMigratorConfig(config)
		val kafkaUtil = new FlinkKafkaConnector(publishConfig)
		val httpUtil = new HttpUtil
		val task = new QumlMigratorStreamTask(publishConfig, kafkaUtil, httpUtil)
		task.process()
	}
}
