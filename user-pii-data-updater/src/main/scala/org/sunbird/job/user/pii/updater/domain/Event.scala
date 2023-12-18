package org.sunbird.job.user.pii.updater.domain

import org.apache.commons.lang3.StringUtils
import org.sunbird.job.domain.reader.JobRequest

import scala.collection.JavaConverters._

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

  private val jobName = "user-pii-data-updater"
  private val validEventAction = List("delete-user")

  def eventId: String = readOrDefault[String]("mid", "")

  def eData: Map[String, AnyRef] = readOrDefault("edata", new java.util.HashMap[String, AnyRef]()).asScala.toMap

  def action: String = readOrDefault[String]("edata.action", "")

  def userId: String = readOrDefault[String]("edata.userId", "")

  def userName: String = readOrDefault[String]("edata.userName", "user")

  def objType: String = readOrDefault[String]("object.type", "")

  def validEvent(): Boolean = {
    validEventAction.contains(action) && StringUtils.isNotBlank(userId)
  }

  def orgAdminUserId(): List[String] = {
    val suggestedUsers: List[Map[String, AnyRef]] = readOrDefault("edata.suggested_users", List()).asInstanceOf[List[Map[String, AnyRef]]]
    val adminIds: List[String] = suggestedUsers.filter(su => StringUtils.equalsIgnoreCase("ORG_ADMIN", su.getOrElse("role", "").asInstanceOf[String])).flatMap(oa => oa.getOrElse("users", List()).asInstanceOf[List[String]])
    adminIds
  }
}
