package org.sunbird.job.questionset.function

import com.google.gson.reflect.TypeToken
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.questionset.publish.domain.{Event, PublishMetadata}
import org.sunbird.job.questionset.task.QuestionSetPublishConfig
import org.sunbird.job.util.LoggerUtil
import org.sunbird.job.{BaseProcessFunction, Metrics}

import java.lang.reflect.Type

class PublishEventRouter(config: QuestionSetPublishConfig) extends BaseProcessFunction[Event, String](config) {


	private[this] val logger = LoggerFactory.getLogger(classOf[PublishEventRouter])
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
		val requestId = event.getEventContext().getOrElse("requestId", "").asInstanceOf[String]
		val entryMsg = s"""Received Event For Publish. | Event : ${event}"""
		logger.info(LoggerUtil.getEntryLogs(config.jobName, requestId, entryMsg))
		if (event.validEvent()) {
			event.objectType match {
				case "Question" | "QuestionImage" => {
					logger.info(s"PublishEventRouter :: Sending Question For Publish Having Identifier: ${event.objectId} | requestId : ${requestId}")
					context.output(config.questionPublishOutTag, PublishMetadata(event.getEventContext(), event.objectId, event.objectType, event.mimeType, event.pkgVersion, event.publishType, event.lastPublishedBy, event.schemaVersion))
				}
				case "QuestionSet" | "QuestionSetImage" => {
					logger.info(s"PublishEventRouter :: Sending QuestionSet For Publish Having Identifier: ${event.objectId} | requestId : ${requestId}")
					context.output(config.questionSetPublishOutTag, PublishMetadata(event.getEventContext(), event.objectId, event.objectType, event.mimeType, event.pkgVersion, event.publishType, event.lastPublishedBy, event.schemaVersion))
				}
				case _ => {
					metrics.incCounter(config.skippedEventCount)
					val exitMsg = s"""Invalid Object Type Received For Publish.| Identifier : ${event.objectId} , objectType : ${event.objectType}"""
					logger.info(LoggerUtil.getExitLogs(config.jobName, requestId, exitMsg))
				}
			}
		} else {
			val exitMsg = s"""Event skipped for identifier: ${event.objectId} , objectType: ${event.objectType}"""
			logger.info(LoggerUtil.getExitLogs(config.jobName, requestId, exitMsg))
			metrics.incCounter(config.skippedEventCount)
		}


	}
}
