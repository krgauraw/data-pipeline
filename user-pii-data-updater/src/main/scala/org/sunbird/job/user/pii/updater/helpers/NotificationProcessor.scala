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
          val url = config.userorg_service_baseUrl + "/v2/notification"
          val body = getRequestBody(data, userId, userName, orgAdminIds)
          val header: Map[String, String] = Map("Content-Type" -> "application/json", "Accept" -> "application/json")
          logger.info(s"url : ${url} | request body : ${body} ")
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
    val adminIdStr: String = orgAdminIds.map(s => s""""$s"""").mkString(", ")
    val req =
      s"""{
         |    "request": {
         |        "type": "email",
         |        "subject": "${config.notification_email_subject}",
         |        "body": "${body}",
         |        "regards": "${config.notification_email_regards}",
         |        "recipientUserIds": [${adminIdStr}]
         |    }
         |}
         |""".stripMargin
    req
  }

  def getHtmlBody(data: Map[String, String], userId: String, userName: String): String = {
    val htmlBody = s"""<html lang=\\"en\\"><head><meta charset=\\"UTF-8\\"><meta name=\\"viewport\\" content=\\"width=device-width initial-scale=1.0\\"><style>body {font-family: Arial, sans-serif;margin: 0;padding: 0;display: flex;flex-direction: column;align-items: center;justify-content: center;background-color: #f4f4f4;}.notification {max-width: 600px;background-color: #fff;padding: 20px;border-radius: 8px;box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);margin-bottom: 20px;}.table-container {width: 100%;overflow: auto;}table {width: 100%;border-collapse: collapse;margin-top: 20px;}th,td {border: 1px solid #ddd;padding: 8px;text-align: left;}th {background-color: #f2f2f2;}</style><title>Table Styling Example</title></head><body><div class=\\"notification\\"><p class=\\"notification-text\\">This notification is to inform you about the recent deletion of a user account having user id:  ${userId}.</p><p class=\\"user-text\\">Ownership of the assets previously held by ${userName} on the platform will need to be reassigned.</p><p class=\\"necessary-text\\">Please take the necessary steps to ensure a smooth transition of ownership for any critical assets.</p><div class=\\"table-container\\">TABLE_DATA</div></div></body></html>""".stripMargin
    val distinctValues = data.values.toSet
    val htmlTable = new java.lang.StringBuilder()
    htmlTable.append("<table><thead><tr><th>Identifier</th><th>Status</th></tr></thead><tbody>")
    distinctValues.foreach { value =>
      val entriesForValue = data.filter { case (_, status) => status == value }
      entriesForValue.foreach { case (key, value) =>
        htmlTable.append("<tr>")
        htmlTable.append("<td>").append(key).append("</td>")
        htmlTable.append("<td>").append(value.asInstanceOf[String]).append("</td>")
        htmlTable.append("</tr>")
      }
    }
    htmlTable.append("</tbody></table>")
    val updatedBody = htmlBody.replace("TABLE_DATA", htmlTable.toString)
    updatedBody
  }
}
