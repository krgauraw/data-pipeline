package org.sunbird.job.user.pii.updater.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.job.BaseJobConfig
import org.sunbird.job.user.pii.updater.domain.UserPiiEvent

import java.util

class UserPiiUpdaterConfig (override val config: Config) extends BaseJobConfig(config, "user-pii-data-updater") {

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
  implicit val migrationMetaTypeInfo: TypeInformation[UserPiiEvent] = TypeExtractor.getForClass(classOf[UserPiiEvent])

  // Job Configuration
  val jobEnv: String = config.getString("job.env")

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  val inputConsumerName = "user-pii-data-updater-consumer"

  // Parallelism
  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
  val eventRouterParallelism: Int = config.getInt("task.router.parallelism")
  val userPiiDataUpdaterParallelism: Int = if(config.hasPath("task.user_pii_data_updater.parallelism")) config.getInt("task.user_pii_data_updater.parallelism") else 1

  // Metric List
  val totalEventsCount = "total-events-count"
  val skippedEventCount = "skipped-event-count"
  val userPiiUpdateCount = "user-pii-update-count"
  val userPiiUpdateSuccessEventCount = "user-pii-update-success-count"
  val userPiiUpdateFailedEventCount = "user-pii-update-failed-count"
  val userPiiUpdateSkippedEventCount = "user-pii-update-skipped-count"
  val userPiiUpdatePartialSuccessEventCount = "user-pii-update-partial-success-count"

  // Neo4J Configurations
  val graphRoutePath = config.getString("neo4j.routePath")
  val graphName = config.getString("neo4j.graph")

  // Out Tags
  val userPiiEventOutTag: OutputTag[UserPiiEvent] = OutputTag[UserPiiEvent]("user-pii-event")

  val target_object_types: util.Map[String, AnyRef] = if(config.hasPath("target_object_types")) config.getAnyRef("target_object_types").asInstanceOf[util.Map[String, AnyRef]] else new util.HashMap[String, AnyRef]()
  val user_pii_replacement_value: String = config.getString("user_pii_replacement_value")
  val definitionBasePath: String = if (config.hasPath("schema.basePath")) config.getString("schema.basePath") else "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/schemas/local"
  val admin_email_notification_enable = if(config.hasPath("admin_email_notification_enable")) config.getBoolean("admin_email_notification_enable") else true
  val userorg_service_baseUrl = config.getString("userorg_service_base_url")
  val notification_email_subject = if(config.hasPath("notification.email.subject")) config.getString("notification.email.subject") else "User Account Deletion Notification"
  val notification_email_regards = if(config.hasPath("notification.email.regards")) config.getString("notification.email.regards") else "Team"

}
