package org.sunbird.job.user.pii.updater.helpers

import org.slf4j.LoggerFactory
import org.sunbird.job.exception.ServerException
import org.sunbird.job.user.pii.updater.task.UserPiiUpdaterConfig
import org.sunbird.job.util.{HTTPResponse, HttpUtil}

import scala.collection.JavaConverters._

trait NotificationProcessor {

  private[this] val logger = LoggerFactory.getLogger(classOf[NotificationProcessor])

  def sendNotification(data: Map[String, String], userId: String, userName: String, orgAdminIds: List[String])(implicit config: UserPiiUpdaterConfig, httpUtil: HttpUtil): Unit = {
    if (config.admin_email_notification_enable) {
      orgAdminIds.isEmpty match {
        case true => logger.info(s"NotificationProcessor ::: sendNotification ::: Org Admin Ids Not Available. So Notification Skipped for userId : ${userId}")
        case false => {
          logger.info(s"NotificationProcessor ::: sendNotification ::: Notification will be triggered to admin user ids ::: ${orgAdminIds}")
          val url = config.userorg_service_baseUrl + "/userorg/v2/notification"
          val body = getRequestBody(data, userId, userName, orgAdminIds)
          val header: Map[String, String] = Map("Content-Type" -> "application/json", "Accept" -> "application/json")
          val httpResponse: HTTPResponse = httpUtil.post(url, body, header)
          if (httpResponse.status == 200) {
            logger.info(s"NotificationProcessor ::: sendNotification ::: Notification sent successfully to admin user ids : ${orgAdminIds} for user id: ${userId}")
          } else {
            logger.info(s"NotificationProcessor ::: sendNotification ::: Notification could not sent to admin user ids : ${orgAdminIds} for user id: ${userId}")
            throw new ServerException("ERR_NOTIFICATION_API_CALL", s"Invalid Response received while sending notification for user: ${userId} | Response Code: ${httpResponse.status} , Response Body:  " + httpResponse.body)
          }
        }
      }
    } else {
      logger.info(s"NotificationProcessor ::: sendNotification ::: notification disabled for the job")
    }
  }

  def getRequestBody(data: Map[String, String], userId: String, userName: String, orgAdminIds: List[String])(implicit config: UserPiiUpdaterConfig): String = {
    val body = getHtmlBody(data, userId, userName)
    val req =
      s"""{
         |    "request": {
         |        "type": "email",
         |        "subject": "${config.notification_email_subject}",
         |        "body": "${body}",
         |        "regards": "${config.notification_email_regards}",
         |        "recipientUserIds": ${orgAdminIds.asJava}
         |    }
         |}
         |""".stripMargin
    req
  }

  def getHtmlBody(data: Map[String, String], userId: String, userName: String): String = {
    //TODO: Return full html body contains all text and data
    s"Deleted User Id: ${userId}. Affected Identifiers: ${data.keySet}"
  }
}
