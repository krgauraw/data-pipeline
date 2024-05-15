package org.sunbird.job.user.pii.updater.domain

import org.apache.commons.lang3.StringUtils
import org.sunbird.job.domain.reader.JobRequest

import scala.collection.JavaConverters._

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

  private val jobName = "user-pii-data-updater"
  private val validEventAction = List("delete-user", "ownership-transfer")

  def eventId: String = readOrDefault[String]("mid", "")

  def eData: Map[String, AnyRef] = readOrDefault("edata", new java.util.HashMap[String, AnyRef]()).asScala.toMap

  def action: String = readOrDefault[String]("edata.action", "")

  def userId: String = readOrDefault[String]("edata.userId", "")

  def userName: String = readOrDefault[String]("edata.userName", "user")

  def objType: String = readOrDefault[String]("object.type", "")

  def fromUserProfile: Map[String, AnyRef] = readOrDefault[Map[String, AnyRef]]("edata.fromUserProfile", Map.empty[String, AnyRef])

  def toUserProfile: Map[String, AnyRef] = readOrDefault[Map[String, AnyRef]]("edata.toUserProfile", Map.empty[String, AnyRef])

  def assetInformation: Map[String, AnyRef] = readOrDefault[Map[String, AnyRef]]("edata.assetInformation", Map.empty[String, AnyRef])

  def fromUserId: String = readOrDefault[String]("edata.fromUserProfile.userId", "user")
  def toUserId: String = readOrDefault[String]("edata.toUserProfile.userId", "user")

  def validEvent(): Boolean = {
    validEventAction.contains(action) && StringUtils.equalsIgnoreCase("User", objType) && (action match {
      case "delete-user" => StringUtils.isNotBlank(userId)
      case "ownership-transfer" => validateFromUserProfile(fromUserProfile) && validateToUserProfile(toUserProfile)
      case _ => false
    })
  }

  def validateFromUserProfile(data: Map[String, AnyRef]): Boolean = {
    val userId: String = data.getOrElse("userId", "").asInstanceOf[String]
    StringUtils.isNotBlank(userId)
  }

  def validateToUserProfile(data: Map[String, AnyRef]): Boolean = {
    val userId: String = data.getOrElse("userId", "").asInstanceOf[String]
    val firstName: String = data.getOrElse("firstName", "").asInstanceOf[String]
    val lastName: String = data.getOrElse("lastName", "").asInstanceOf[String]
    val roles: List[String] = data.getOrElse("roles", List()).asInstanceOf[List[String]]
    StringUtils.isNotBlank(userId) && StringUtils.isNotBlank(firstName) && StringUtils.isNotBlank(lastName) && !roles.isEmpty
  }

  def orgAdminUserId(): List[String] = {
    val suggestedUsers: List[Map[String, AnyRef]] = readOrDefault("edata.suggested_users", List()).asInstanceOf[List[Map[String, AnyRef]]]
    val adminIds: List[String] = suggestedUsers.filter(su => StringUtils.equalsIgnoreCase("ORG_ADMIN", su.getOrElse("role", "").asInstanceOf[String])).flatMap(oa => oa.getOrElse("users", List()).asInstanceOf[List[String]])
    adminIds
  }

  def getEventContext(): Map[String, AnyRef] = {
    val mid: String = readOrDefault[String]("mid", "")
    val requestId: String = readOrDefault[String]("edata.requestId", "")
    val defaultFeature = readOrDefault[String]("edata.action", "") match {
      case "delete-user" => "DeleteUser"
      case "ownership-transfer" => "OwnershipTransfer"
      case _ => readOrDefault[String]("edata.action", "")
    }
    val featureName: String = readOrDefault[String]("edata.featureName", defaultFeature)
    Map("mid" -> mid, "requestId" -> requestId, "featureName" -> featureName)
  }
}
