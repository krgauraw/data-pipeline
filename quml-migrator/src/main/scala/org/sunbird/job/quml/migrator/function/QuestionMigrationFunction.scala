package org.sunbird.job.quml.migrator.function

import akka.dispatch.ExecutionContexts
import com.google.gson.reflect.TypeToken
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.domain.`object`.{DefinitionCache, ObjectDefinition}
import org.sunbird.job.quml.migrator.domain.{ExtDataConfig, MigrationMetadata, ObjectData}
import java.util.UUID
import org.sunbird.job.util.{CassandraUtil, HttpUtil, Neo4JUtil}
import org.sunbird.job.{BaseProcessFunction, Metrics}
import java.lang.reflect.Type
import org.sunbird.job.quml.migrator.helpers.QuestionMigrator
import org.sunbird.job.quml.migrator.task.QumlMigratorConfig
import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext

class QuestionMigrationFunction(config: QumlMigratorConfig, httpUtil: HttpUtil,
                                @transient var neo4JUtil: Neo4JUtil = null,
                                @transient var cassandraUtil: CassandraUtil = null,
                                @transient var definitionCache: DefinitionCache = null)
                               (implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[MigrationMetadata, String](config) with QuestionMigrator {

  private[this] val logger = LoggerFactory.getLogger(classOf[QuestionMigrationFunction])
  val mapType: Type = new TypeToken[java.util.Map[String, AnyRef]]() {}.getType
  private val statusList = List("Live", "Unlisted")

  @transient var ec: ExecutionContext = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    cassandraUtil = new CassandraUtil(config.cassandraHost, config.cassandraPort, config)
    neo4JUtil = new Neo4JUtil(config.graphRoutePath, config.graphName, config)
    ec = ExecutionContexts.global
    definitionCache = new DefinitionCache()
  }

  override def close(): Unit = {
    super.close()
    cassandraUtil.close()
  }

  override def metricsList(): List[String] = {
    List(config.questionMigrationCount, config.questionMigrationSuccessEventCount, config.questionMigrationFailedEventCount, config.questionRepublishEventCount)
  }

  override def processElement(data: MigrationMetadata, context: ProcessFunction[MigrationMetadata, String]#Context, metrics: Metrics): Unit = {
    logger.info("Question Migration started for : " + data.identifier)
    metrics.incCounter(config.questionMigrationCount)
    val definition: ObjectDefinition = definitionCache.getDefinition(data.objectType, data.schemaVersion, config.definitionBasePath)
    val readerConfig = ExtDataConfig(config.questionKeyspaceName, definition.getExternalTable, definition.getExternalPrimaryKey, definition.getExternalProps)
    val objData = getObject(data.identifier, readerConfig)(neo4JUtil, cassandraUtil, config)
    val messages: List[String] = validateQuestion(data.identifier, objData)(config)
    if(messages.isEmpty) {
      val migratedObj: ObjectData = migrateQuestion(objData)(definition).getOrElse(objData)
      val status = migratedObj.metadata.getOrElse("status", "").asInstanceOf[String]
      val qumlVersion: Double = migratedObj.metadata.getOrElse("qumlVersion", 1.0).asInstanceOf[Double]
      val migrationVersion: Double = migratedObj.metadata.getOrElse("migrationVersion", 0.0).asInstanceOf[Double]
      if (migrationVersion == 3.0 && qumlVersion == 1.1) {
        val upgradedQumlDef: ObjectDefinition = definitionCache.getDefinition(data.objectType, qumlVersion.toString, config.definitionBasePath)
        val qumlReaderConfig =  ExtDataConfig(config.questionKeyspaceName, upgradedQumlDef.getExternalTable, upgradedQumlDef.getExternalPrimaryKey, upgradedQumlDef.getExternalProps)
        saveOnSuccess(migratedObj)(neo4JUtil, cassandraUtil, qumlReaderConfig, definitionCache, config)
        metrics.incCounter(config.questionMigrationSuccessEventCount)
        logger.info("Question Migration Successful For : " + data.identifier)
        if (statusList.contains(status)) {
          pushQuestionPublishEvent(migratedObj.metadata, context, metrics, config)
          logger.info("Question Re Publish Event Triggered Successfully For : " + data.identifier)
        }
      } else {
        logger.info("Question Migration Failed For : " + data.identifier + " | Errors : " + messages.mkString("; "))
        val errorMessages = messages.mkString("; ")
        val metadata = objData.metadata ++ Map[String, AnyRef]("migrationVersion" -> 2.1.asInstanceOf[AnyRef], "migrationError" -> errorMessages)
        val newObj = new ObjectData(objData.identifier, metadata, objData.extData, objData.hierarchy)
        saveOnFailure(newObj)(neo4JUtil)
        metrics.incCounter(config.questionMigrationFailedEventCount)
      }
    } else {
      val errorMessages = messages.mkString("; ")
      val metadata = objData.metadata ++ Map[String, AnyRef]("migrationVersion"->2.3.asInstanceOf[AnyRef], "migrationError" -> errorMessages)
      val newObj = new ObjectData(objData.identifier, metadata, objData.extData, objData.hierarchy)
      logger.info("Question Migration Skipped For : " + data.identifier + " | Errors : " + messages.mkString("; "))
      saveOnFailure(newObj)(neo4JUtil)
      metrics.incCounter(config.questionMigrationSkippedEventCount)
    }
  }

  def validateQuestion(identifier: String, obj: ObjectData)(implicit config: QumlMigratorConfig): List[String] = {
    logger.info("Validating object with id: " + obj.identifier)
    val messages = ListBuffer[String]()
    if (obj.metadata.isEmpty) messages += s"""There is no metadata available for : $identifier"""
    if (obj.metadata.get("mimeType").isEmpty) messages += s"""There is no mimeType defined for : $identifier"""
    if (obj.metadata.get("primaryCategory").isEmpty) messages += s"""There is no primaryCategory defined for : $identifier"""
    if (obj.extData.getOrElse(Map()).getOrElse("body", "").asInstanceOf[String].isEmpty) messages += s"""There is no body available for : $identifier"""
    val interactionTypes = obj.metadata.getOrElse("interactionTypes", new util.ArrayList[String]()).asInstanceOf[util.List[String]].asScala.toList
    if (interactionTypes.nonEmpty) {
      if (obj.extData.get.getOrElse("responseDeclaration", "").asInstanceOf[String].isEmpty) messages += s"""There is no responseDeclaration available for : $identifier"""
      if (obj.extData.get.getOrElse("interactions", "").asInstanceOf[String].isEmpty) messages += s"""There is no interactions available for : $identifier"""
    } else {
      if (obj.extData.getOrElse(Map()).getOrElse("answer", "").asInstanceOf[String].isEmpty) messages += s"""There is no answer available for : $identifier"""
    }
    messages.toList
  }

  def pushQuestionPublishEvent(objMetadata: Map[String, AnyRef], context: ProcessFunction[MigrationMetadata, String]#Context, metrics: Metrics, config: QumlMigratorConfig): Unit = {
    val epochTime = System.currentTimeMillis
    val identifier = objMetadata.getOrElse("identifier", "").asInstanceOf[String]
    val pkgVersion = objMetadata.getOrElse("pkgVersion", "").asInstanceOf[Number]
    val objectType = objMetadata.getOrElse("objectType", "").asInstanceOf[String]
    val mimeType = objMetadata.getOrElse("mimeType", "").asInstanceOf[String]
    val status = objMetadata.getOrElse("status", "").asInstanceOf[String]
    val schemaVersion = objMetadata.getOrElse("schemaVersion", "").asInstanceOf[String]
    val publishType = if (status.equalsIgnoreCase("Live")) "Public" else "Unlisted"
    val channel = objMetadata.getOrElse("channel", "").asInstanceOf[String]
    val lastPublishedBy = objMetadata.getOrElse("lastPublishedBy", "System").asInstanceOf[String]
    val event = s"""{"eid":"BE_JOB_REQUEST","ets":$epochTime,"mid":"LP.$epochTime.${UUID.randomUUID()}","actor":{"id":"question-republish","type":"System"},"context":{"pdata":{"ver":"1.0","id":"org.sunbird.platform"},"channel":"${channel}","env":"${config.jobEnv}"},"object":{"ver":"$pkgVersion","id":"$identifier"},"edata":{"publish_type":"$publishType","metadata":{"identifier":"$identifier", "mimeType":"$mimeType","objectType":"$objectType","lastPublishedBy":"${lastPublishedBy}","pkgVersion":$pkgVersion,"schemaVersion":$schemaVersion},"action":"republish","iteration":1}}"""
    logger.info(s"QuestionMigrationFunction :: Live ${objectType} re-publish triggered for " + identifier)
    logger.info(s"QuestionMigrationFunction :: Live ${objectType} re-publish event: " + event)
    context.output(config.liveNodePublishEventOutTag, event)
    metrics.incCounter(config.questionRepublishEventCount)
  }
}
