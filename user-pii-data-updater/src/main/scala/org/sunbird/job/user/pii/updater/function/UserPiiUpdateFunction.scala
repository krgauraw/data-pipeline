package org.sunbird.job.user.pii.updater.function

import akka.dispatch.ExecutionContexts
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.domain.`object`.{DefinitionCache, ObjectDefinition}
import org.sunbird.job.user.pii.updater.domain.{ObjectData, UserPiiEvent}
import org.sunbird.job.user.pii.updater.helpers.{Neo4jDataProcessor, UserPiiUpdater}
import org.sunbird.job.user.pii.updater.task.UserPiiUpdaterConfig
import org.sunbird.job.util.{HttpUtil, LoggerUtil, Neo4JUtil}
import org.sunbird.job.{BaseProcessFunction, Metrics}

import java.util
import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext

class UserPiiUpdateFunction(config: UserPiiUpdaterConfig, httpUtil: HttpUtil,
                            @transient var neo4JUtil: Neo4JUtil = null,
                            @transient var definitionCache: DefinitionCache = null)
                           (implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[UserPiiEvent, String](config) with Neo4jDataProcessor with UserPiiUpdater {

  private[this] val logger = LoggerFactory.getLogger(classOf[UserPiiUpdateFunction])

  @transient var ec: ExecutionContext = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    definitionCache = new DefinitionCache()
    neo4JUtil = new Neo4JUtil(config.graphRoutePath, config.graphName, config)
    ec = ExecutionContexts.global
    definitionCache = new DefinitionCache()
  }

  override def close(): Unit = {
    super.close()
  }

  override def metricsList(): List[String] = {
    List(config.userPiiUpdateSuccessEventCount, config.userPiiUpdateFailedEventCount, config.userPiiUpdateSkippedEventCount, config.userPiiUpdatePartialSuccessEventCount)
  }

  override def processElement(userEvent: UserPiiEvent, context: ProcessFunction[UserPiiEvent, String]#Context, metrics: Metrics): Unit = {
    val requestId = userEvent.eventContext.getOrElse("requestId", "").asInstanceOf[String]
    val featureName = userEvent.eventContext.getOrElse("featureName", "").asInstanceOf[String]
    logger.info(s"Feature: ${featureName} | UserPiiUpdateFunction event: ${userEvent} | requestId: ${requestId}")
    try {
      val idMap = new util.HashMap[String, String]()
      val failedIdMap = new util.HashMap[String, String]()
      val targetObjectTypes: Map[String, AnyRef] = config.target_object_types.toMap
      targetObjectTypes.foreach(entry => {
        val schemaVersions: List[String] = entry._2.asInstanceOf[java.util.List[String]].toList
        schemaVersions.foreach(ver => {
          val definition: ObjectDefinition = definitionCache.getDefinition(entry._1, ver, config.definitionBasePath)
          val userPiiFields = definition.getPiiFields(userEvent.objectType.toLowerCase())
          userPiiFields.foreach(pii => {
            val targetKeys = pii._2.asInstanceOf[List[String]]
            logger.info(s"Feature: ${featureName} | UserPiiUpdateFunction :: Processing for pii update : userId: ${userEvent.userId} , objectType : ${entry._1} , schemaVersion : ${ver} , pii_field_config: ${pii}, requestId: ${requestId}")
            val nodes: List[ObjectData] = searchObjects(entry._1, pii._1, ver, userEvent.userId, pii._2.asInstanceOf[List[String]])(neo4JUtil)
            if (!nodes.isEmpty) {
              nodes.map(node => {
                logger.info(s"Feature: ${featureName} | UserPiiUpdateFunction ::: processing node with metadata ::: ${node.metadata} | requestId: ${requestId}")
                val meta: Map[String, AnyRef] = targetKeys.map(key => {
                  if (StringUtils.contains(key, ".")) {
                    processNestedProp(key, node)(config)
                  } else {
                    Map(key -> config.user_pii_replacement_value)
                  }
                }).flatten.toMap
                logger.info(s"Feature: ${featureName} | UserPiiUpdateFunction ::: metadata going to be updated for ${node.id} ::: ${meta} | requestId: ${requestId}")
                val updatedId = updateObject(node.id, meta)(neo4JUtil)
                logger.info("updatedId ::: " + updatedId)
                if (StringUtils.isNotBlank(updatedId)) {
                  logger.info(s"Feature: ${featureName} | Node Updated Successfully for identifier: ${node.id} | requestId: ${requestId}")
                  if (StringUtils.equalsIgnoreCase("Default", node.metadata.getOrElse("visibility", "").asInstanceOf[String]))
                    idMap.put(node.id.replace(".img", ""), node.status)
                } else {
                  logger.info(s"Feature: ${featureName} | Node Update Failed for identifier: ${node.id} | requestId: ${requestId}")
                  if (StringUtils.equalsIgnoreCase("Default", node.metadata.getOrElse("visibility", "").asInstanceOf[String]))
                    failedIdMap.put(node.id.replace(".img", ""), node.status)
                }
              })
            } else {
              logger.info(s"Feature: ${featureName} | No Object Found For objectType: ${entry._1}, userId: ${userEvent.userId}, lookupKey: ${pii._1} | requestId: ${requestId}")
            }
          })
        })
      })
      processResult(userEvent, idMap, failedIdMap, featureName)(config, httpUtil, metrics)
    } catch {
      case e: Throwable => {
        val errCode = "ERR_UPID_USER_PII_UPDATE_FAILED"
        val errorDesc = s"SYSTEM_ERROR: ${e.getMessage}"
        val stackTrace: String = e.getStackTraceString
        logger.error(LoggerUtil.getErrorLogs(errCode, errorDesc, requestId, stackTrace))
        throw e
      }
    }
  }
}