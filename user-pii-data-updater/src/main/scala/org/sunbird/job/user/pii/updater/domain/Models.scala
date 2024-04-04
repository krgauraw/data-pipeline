package org.sunbird.job.user.pii.updater.domain

case class UserPiiEvent(eventId: String, objectType: String, userId: String, userName: String, orgAdminUserId: List[String])

case class OwnershipTransferEvent(eventId: String, objectType: String, fromUserProfile: Map[String, AnyRef], toUserProfile: Map[String, AnyRef], assetInfo: Map[String, AnyRef], fromUserId: String, toUserId: String)

class ObjectData(val identifier: String, val metadata: Map[String, AnyRef]) {
  val id: String = metadata.getOrElse("identifier", identifier).asInstanceOf[String]
  val objectType: String = metadata.getOrElse("objectType", "").asInstanceOf[String]
  val status: String = metadata.getOrElse("status", "").asInstanceOf[String]
  def getString(key: String, defaultVal: String): String = metadata.getOrElse(key, defaultVal).asInstanceOf[String]
}
