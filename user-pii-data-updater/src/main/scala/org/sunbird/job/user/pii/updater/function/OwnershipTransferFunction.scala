package org.sunbird.job.user.pii.updater.function

import akka.dispatch.ExecutionContexts
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.domain.`object`.{DefinitionCache, ObjectDefinition}
import org.sunbird.job.user.pii.updater.domain.{ObjectData, OwnershipTransferEvent}
import org.sunbird.job.user.pii.updater.helpers.{Neo4jDataProcessor, UserPiiUpdater}
import org.sunbird.job.user.pii.updater.task.UserPiiUpdaterConfig
import org.sunbird.job.util.{HttpUtil, LoggerUtil, Neo4JUtil}
import org.sunbird.job.{BaseProcessFunction, Metrics}

import java.util
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

class OwnershipTransferFunction(config: UserPiiUpdaterConfig, httpUtil: HttpUtil,
                                @transient var neo4JUtil: Neo4JUtil = null,
                                @transient var definitionCache: DefinitionCache = null)
                               (implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[OwnershipTransferEvent, String](config) with Neo4jDataProcessor with UserPiiUpdater {

  private[this] val logger = LoggerFactory.getLogger(classOf[OwnershipTransferFunction])

  @transient var ec: ExecutionContext = _
  val featureId = "OwnershipTransfer"

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    definitionCache = new DefinitionCache()
    neo4JUtil = new Neo4JUtil(config.graphRoutePath, config.graphName, config)
    ec = ExecutionContexts.global
  }

  override def close(): Unit = {
    super.close()
  }

  override def metricsList(): List[String] = {
    List(config.ownershipTransferSuccessEventCount, config.ownershipTransferFailedEventCount, config.ownershipTransferSkippedEventCount)
  }

  override def processElement(event: OwnershipTransferEvent, context: ProcessFunction[OwnershipTransferEvent, String]#Context, metrics: Metrics): Unit = {
    val requestId = event.eventContext.getOrElse("requestId", "").asInstanceOf[String]
    logger.info(s"Feature: ${featureId} | OwnershipTransferFunction event: ${event} | requestId: ${requestId}")
    try {
      val toUserRoles: List[String] = event.toUserProfile.getOrElse("roles", List()).asInstanceOf[List[String]]
      if (isValidUserRole(toUserRoles)(config)) {
        val idMap = new util.HashMap[String, String]()
        val failedIdMap = new util.HashMap[String, String]()
        if (null != event.assetInfo.isEmpty && !event.assetInfo.isEmpty) {
          val assetId = event.assetInfo.getOrElse("identifier", "").asInstanceOf[String]
          val objectType = event.assetInfo.getOrElse("objectType", "").asInstanceOf[String]
          logger.info(s"Feature: ${featureId} | OwnershipTransferFunction :: Processing objectType : ${objectType} , Identifier : ${assetId} for ownership transfer between :: from userId: ${event.fromUserId} , to userId: ${event.toUserId} | requestId: ${requestId}")
          val node = neo4JUtil.getNodeProperties(assetId)
          if (null != node) {
            val schemaVersion = node.getOrElse("schemaVersion", "1.0").asInstanceOf[String]
            val definition: ObjectDefinition = definitionCache.getDefinition(objectType, schemaVersion, config.definitionBasePath)
            val userPiiFields = definition.getPiiFields(event.objectType.toLowerCase())
            val nodeId = node.getOrElse("identifier", assetId).asInstanceOf[String]
            val toUserName = event.toUserProfile.getOrElse("firstName", "").asInstanceOf[String] + " " + event.toUserProfile.getOrElse("lastName", "").asInstanceOf[String]
            userPiiFields.foreach(pii => {
              val meta: Map[String, AnyRef] = Map(pii._1 -> event.toUserId, pii._2.asInstanceOf[List[String]].head -> toUserName)
              val updatedId = updateObject(nodeId, meta)(neo4JUtil)
              if (StringUtils.isNotBlank(updatedId)) {
                logger.info(s"Feature: ${featureId} | Node Updated Successfully for identifier: ${nodeId} | requestId: ${requestId}")
                if (StringUtils.equalsIgnoreCase("Default", node.getOrElse("visibility", "").asInstanceOf[String]))
                  idMap.put(nodeId.replace(".img", ""), node.getOrElse("status", "").asInstanceOf[String])
              } else {
                logger.info(s"Feature: ${featureId} | Node Update Failed for identifier: ${nodeId} | requestId: ${requestId}")
                if (StringUtils.equalsIgnoreCase("Default", node.getOrElse("visibility", "").asInstanceOf[String]))
                  failedIdMap.put(nodeId.replace(".img", ""), node.getOrElse("status", "").asInstanceOf[String])
              }
            })
          } else {
            val exitMsg = s"Feature: ${featureId} | No Object Found with Identifier ${assetId}, objectType: ${objectType} having ownership  with user id : ${event.fromUserId}"
            logger.info(LoggerUtil.getExitLogs(config.jobName, requestId, exitMsg))
          }
        } else {
          val targetObjectTypes: Map[String, AnyRef] = config.target_object_types.toMap
          targetObjectTypes.foreach(entry => {
            val schemaVersions: List[String] = entry._2.asInstanceOf[java.util.List[String]].toList
            schemaVersions.foreach(ver => {
              val definition: ObjectDefinition = definitionCache.getDefinition(entry._1, ver, config.definitionBasePath)
              val userPiiFields = definition.getPiiFields(event.objectType.toLowerCase())
              val toUserName = event.toUserProfile.getOrElse("firstName", "").asInstanceOf[String] + " " + event.toUserProfile.getOrElse("lastName", "").asInstanceOf[String]
              userPiiFields.foreach(pii => {
                logger.info(s"Feature: ${featureId} | OwnershipTransferFunction :: Processing objectType : ${entry._1} , schemaVersion : ${ver} for ownership transfer between :: from userId: ${event.fromUserId} , to userId: ${event.toUserId} | requestId: ${requestId}")
                val nodes: List[ObjectData] = searchObjects(entry._1, pii._1, ver, event.fromUserId)(neo4JUtil)
                if (!nodes.isEmpty) {
                  logger.info(s"Feature: ${featureId} | OwnershipTransferFunction ::: ${nodes.size} nodes found for ownership transfer. | requestId: ${requestId}")
                  nodes.map(node => {
                    logger.info(s"Feature: ${featureId} | OwnershipTransferFunction ::: processing node with metadata ::: ${node.metadata} | requestId: ${requestId}")
                    val meta: Map[String, AnyRef] = Map(pii._1 -> event.toUserId, pii._2.asInstanceOf[List[String]].head -> toUserName)
                    logger.info(s"Feature: ${featureId} | OwnershipTransferFunction ::: metadata going to be updated for ${node.id} ::: ${meta} | requestId: ${requestId}")
                    val updatedId = updateObject(node.id, meta)(neo4JUtil)
                    logger.info("updatedId ::: " + updatedId)
                    if (StringUtils.isNotBlank(updatedId)) {
                      logger.info(s"Feature: ${featureId} | Node Updated Successfully for identifier: ${node.id} | requestId: ${requestId}")
                      if (StringUtils.equalsIgnoreCase("Default", node.metadata.getOrElse("visibility", "").asInstanceOf[String]))
                        idMap.put(node.id.replace(".img", ""), node.status)
                    } else {
                      logger.info(s"Feature: ${featureId} | Node Update Failed for identifier: ${node.id} | requestId: ${requestId}")
                      if (StringUtils.equalsIgnoreCase("Default", node.metadata.getOrElse("visibility", "").asInstanceOf[String]))
                        failedIdMap.put(node.id.replace(".img", ""), node.status)
                    }
                  })
                } else {
                  logger.info(s"Feature: ${featureId} | No Object Found For objectType: ${entry._1}, userId: ${event.fromUserId}, lookupKey: ${pii._1} | requestId: ${requestId}")
                }
              })
            })
          })
        }
        processOwnershipTransferResult(event, idMap, failedIdMap, featureId)(config, httpUtil, metrics)
      } else {
        val exitMsg = s"Feature: ${featureId} | To User does not have any valid roles. | toUser Roles: ${toUserRoles} | Expected Roles: ${config.ownershipTransferValidRoles.toList}"
        logger.info(LoggerUtil.getExitLogs(config.jobName, requestId, exitMsg))
      }
    } catch {
      case e: Throwable => {
        val errCode = "ERR_UPID_OWNERSHIP_TRANSFER_FAILED"
        val errorDesc = s"SYSTEM_ERROR: ${e.getMessage}"
        val stackTrace: String = e.getStackTraceString
        logger.error(LoggerUtil.getErrorLogs(errCode, errorDesc, requestId, stackTrace))
        throw e
      }
    }
  }


}
