package org.sunbird.job.quml.migrator.function

import com.google.gson.reflect.TypeToken
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.quml.migrator.domain.{Event, MigrationMetadata}
import org.sunbird.job.quml.migrator.task.QumlMigratorConfig
import org.sunbird.job.{BaseProcessFunction, Metrics}

import java.lang.reflect.Type

class QumlMigrationEventRouter(config: QumlMigratorConfig) extends BaseProcessFunction[Event, String](config) {


	private[this] val logger = LoggerFactory.getLogger(classOf[QumlMigrationEventRouter])
	val mapType: Type = new TypeToken[java.util.Map[String, AnyRef]]() {}.getType

	override def open(parameters: Configuration): Unit = {
		super.open(parameters)
	}

	override def close(): Unit = {
		super.close()
	}

	override def metricsList(): List[String] = {
		List(config.skippedEventCount, config.totalEventsCount)
	}

	override def processElement(event: Event, context: ProcessFunction[Event, String]#Context, metrics: Metrics): Unit = {
		metrics.incCounter(config.totalEventsCount)
		if (event.validEvent()) {
			logger.info("QumlMigrationEventRouter :: Event: " + event)
			event.objectType match {
				case "Question" | "QuestionImage" => {
					logger.info("QumlMigrationEventRouter :: Sending Question For QuML Migration Having Identifier: " + event.objectId)
					context.output(config.questionMigrationOutTag, MigrationMetadata(event.objectId, event.objectType, event.mimeType, event.pkgVersion, event.status, event.qumlVersion, event.schemaVersion))
				}
				case "QuestionSet" | "QuestionSetImage" => {
					logger.info("QumlMigrationEventRouter :: Sending QuestionSet For QuML Migration Having Identifier: " + event.objectId)
					context.output(config.questionSetMigrationOutTag, MigrationMetadata(event.objectId, event.objectType, event.mimeType, event.pkgVersion, event.status, event.qumlVersion, event.schemaVersion))
				}
				case _ => {
					metrics.incCounter(config.skippedEventCount)
					logger.info("Invalid Object Type Received For QuML Migration.| Identifier : " + event.objectId + " , objectType : " + event.objectType)
				}
			}
		} else {
      logger.warn("Event skipped for identifier: " + event.objectId + " objectType: " + event.objectType)
			metrics.incCounter(config.skippedEventCount)
		}
	}
}
