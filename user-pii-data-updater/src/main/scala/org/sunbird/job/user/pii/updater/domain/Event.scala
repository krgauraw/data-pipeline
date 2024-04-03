package org.sunbird.job.user.pii.updater.domain

import org.apache.commons.lang3.StringUtils
import org.sunbird.job.domain.reader.JobRequest

import java.util
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

  def fromUserProfile: Map[String, AnyRef] = eData.getOrElse("fromUserProfile", Map.empty[String, AnyRef]).asInstanceOf[Map[String, AnyRef]]

  def toUserProfile: Map[String, AnyRef] = eData.getOrElse("toUserProfile", Map.empty[String, AnyRef]).asInstanceOf[Map[String, AnyRef]]

  def assetInformation: Map[String, AnyRef] = eData.getOrElse("assetInformation", Map.empty[String, AnyRef]).asInstanceOf[Map[String, AnyRef]]

  def fromUserId: String = fromUserProfile.getOrElse("userId", "").asInstanceOf[String]

  def toUserId: String = toUserProfile.getOrElse("userId", "").asInstanceOf[String]

  def validEvent(): Boolean = {
    println("eData ::: "+eData)
    println("fromUserProfile ::: "+fromUserProfile)
    println("toUserProfile ::: "+toUserProfile)
    println("assetInformation ::: "+assetInformation)
    validEventAction.contains(action) && StringUtils.equalsIgnoreCase("User", objType) && !eData.isEmpty && (action match {
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
    val roles: List[String] = data.getOrElse("roles", new util.ArrayList[String]()).asInstanceOf[java.util.List[String]].asScala.toList
    StringUtils.isNotBlank(userId) && StringUtils.isNotBlank(firstName) && StringUtils.isNotBlank(lastName) && !roles.isEmpty
  }

  def orgAdminUserId(): List[String] = {
    val suggestedUsers: List[Map[String, AnyRef]] = readOrDefault("edata.suggested_users", List()).asInstanceOf[List[Map[String, AnyRef]]]
    val adminIds: List[String] = suggestedUsers.filter(su => StringUtils.equalsIgnoreCase("ORG_ADMIN", su.getOrElse("role", "").asInstanceOf[String])).flatMap(oa => oa.getOrElse("users", List()).asInstanceOf[List[String]])
    adminIds
  }
}
