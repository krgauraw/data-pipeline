package org.sunbird.job.user.pii.updater.function

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.user.pii.updater.domain.{Event, OwnershipTransferEvent, UserPiiEvent}
import org.sunbird.job.user.pii.updater.task.UserPiiUpdaterConfig
import org.sunbird.job.util.LoggerUtil
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
    val requestId = event.getEventContext().getOrElse("requestId", "").asInstanceOf[String]
    val entryMsg = s"""Received Event For user-pii-update / ownership-transfer. | Event : ${event}"""
    logger.info(LoggerUtil.getEntryLogs(config.jobName, requestId, entryMsg))
    if (event.validEvent()) {
      event.action match {
        case "delete-user" => {
          logger.info(s"UserPiiEventRouter :: Sending Event For User Pii Data Cleanup having userId: ${event.userId} | requestId: ${requestId}")
          context.output(config.userPiiEventOutTag, UserPiiEvent(event.getEventContext(), event.eventId, event.objType, event.userId, event.userName, event.orgAdminUserId))
        }
        case "ownership-transfer" => {
          logger.info(s"UserPiiEventRouter :: Sending Event For Ownership Transfer having from_userId: ${event.fromUserId} & to_userId: ${event.toUserId} | requestId: ${requestId}")
          context.output(config.ownershipTransferEventOutTag, OwnershipTransferEvent(event.getEventContext(), event.eventId, event.objType, event.fromUserProfile, event.toUserProfile, event.assetInformation, event.fromUserId, event.toUserId))
        }
        case _ => {
          metrics.incCounter(config.skippedEventCount)
          val exitMsg = s"""Invalid Action Received in the event with mid: ${event.eventId}.| eventId : ${event.eventId} , objectType : ${event.objType}"""
          logger.info(LoggerUtil.getExitLogs(config.jobName, requestId, exitMsg))
        }
      }
    } else {
      val exitMsg = s"""Event Validation Failed. Event skipped for user id: ${event.userId} , objectType: ${event.objType}"""
      logger.info(LoggerUtil.getExitLogs(config.jobName, requestId, exitMsg))
      metrics.incCounter(config.skippedEventCount)
    }
  }
}
