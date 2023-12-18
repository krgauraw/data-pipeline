package org.sunbird.job.user.pii.updater.function

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.user.pii.updater.domain.{Event, UserPiiEvent}
import org.sunbird.job.user.pii.updater.task.UserPiiUpdaterConfig
import org.sunbird.job.{BaseProcessFunction, Metrics}

import scala.collection.JavaConversions._

class UserPiiEventRouter(config: UserPiiUpdaterConfig) extends BaseProcessFunction[Event, String](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[UserPiiEventRouter])

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
    logger.info("UserPiiEventRouter :: Event: " + event)
    if (event.validEvent()) {
      event.action match {
        case "delete-user" => {
          logger.info("UserPiiEventRouter :: Sending Event For User Pii Data Cleanup having userId: " + event.userId)
          context.output(config.userPiiEventOutTag, UserPiiEvent(event.eventId, event.objType, event.userId, event.userName, event.orgAdminUserId))
        }
        case _ => {
          metrics.incCounter(config.skippedEventCount)
          logger.info(s"Invalid Action Received in the event with mid: ${event.eventId}.| Event : ${event}")
        }
      }
    } else {
      logger.info("Event skipped with mid : " + event.eventId)
      metrics.incCounter(config.skippedEventCount)
    }
  }
}
