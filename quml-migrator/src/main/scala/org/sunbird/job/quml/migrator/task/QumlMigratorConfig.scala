package org.sunbird.job.quml.migrator.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.job.BaseJobConfig
import org.sunbird.job.quml.migrator.domain.MigrationMetadata

import java.util

class QumlMigratorConfig(override val config: Config) extends BaseJobConfig(config, "quml-migrator") {

	implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
	implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
	implicit val migrationMetaTypeInfo: TypeInformation[MigrationMetadata] = TypeExtractor.getForClass(classOf[MigrationMetadata])

	// Job Configuration
	val jobEnv: String = config.getString("job.env")

	// Kafka Topics Configuration
	val kafkaInputTopic: String = config.getString("kafka.input.topic")
	val republishTopic: String = config.getString("kafka.republish.topic")
	val inputConsumerName = "quml-migrator-consumer"

	// Parallelism
	override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
	val eventRouterParallelism: Int = config.getInt("task.router.parallelism")

	// Metric List
	val totalEventsCount = "total-events-count"
	val skippedEventCount = "skipped-event-count"
	val questionMigrationCount = "question-migration-count"
	val questionMigrationSuccessEventCount = "question-migration-success-count"
	val questionMigrationFailedEventCount = "question-migration-failed-count"
	val questionMigrationSkippedEventCount = "question-migration-skipped-count"
	val questionSetMigrationEventCount = "questionset-migration-count"
	val questionSetMigrationSuccessEventCount = "questionset-migration-success-count"
	val questionSetMigrationFailedEventCount = "questionset-migration-failed-count"
	val questionSetMigrationSkippedEventCount = "questionset-migration-skipped-count"
	val questionRepublishEventCount = "question-republish-count"
	val questionSetRepublishEventCount = "questionset-republish-count"

	// Cassandra Configurations
	val cassandraHost: String = config.getString("lms-cassandra.host")
	val cassandraPort: Int = config.getInt("lms-cassandra.port")
	val questionKeyspaceName = config.getString("question.keyspace")
	val questionTableName = config.getString("question.table")
	val questionSetKeyspaceName = config.getString("questionset.keyspace")
	val questionSetTableName = config.getString("questionset.table")

	// Neo4J Configurations
	val graphRoutePath = config.getString("neo4j.routePath")
	val graphName = config.getString("neo4j.graph")

	// Out Tags
	val questionMigrationOutTag: OutputTag[MigrationMetadata] = OutputTag[MigrationMetadata]("question-migration")
	val questionSetMigrationOutTag: OutputTag[MigrationMetadata] = OutputTag[MigrationMetadata]("questionset-migration")
	val liveQuestionPublishEventOutTag: OutputTag[String] = OutputTag[String]("live-question-republish-request")
	val liveQuestionSetPublishEventOutTag: OutputTag[String] = OutputTag[String]("live-questionset-republish-request")

	val definitionBasePath: String = if (config.hasPath("schema.basePath")) config.getString("schema.basePath") else "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/schemas/local"

}
