package org.sunbird.job.quml.migrator.function

import akka.dispatch.ExecutionContexts
import com.google.gson.reflect.TypeToken
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.domain.`object`.{DefinitionCache, ObjectDefinition}
import org.sunbird.job.util._
import org.sunbird.job.{BaseProcessFunction, Metrics}
import java.lang.reflect.Type
import org.sunbird.job.quml.migrator.domain.{ExtDataConfig, MigrationMetadata, ObjectData}
import org.sunbird.job.quml.migrator.helpers.QuestionSetMigrator
import org.sunbird.job.quml.migrator.task.QumlMigratorConfig

import scala.concurrent.ExecutionContext

class QuestionSetMigrationFunction(config: QumlMigratorConfig, httpUtil: HttpUtil,
                                   @transient var neo4JUtil: Neo4JUtil = null,
                                   @transient var cassandraUtil: CassandraUtil = null,
                                   @transient var definitionCache: DefinitionCache = null)
                                  (implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[MigrationMetadata, String](config) with QuestionSetMigrator {

  private[this] val logger = LoggerFactory.getLogger(classOf[QuestionSetMigrationFunction])
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
    List(config.questionSetMigrationEventCount, config.questionSetMigrationSuccessEventCount, config.questionSetMigrationFailedEventCount, config.questionSetMigrationSkippedEventCount, config.questionSetRepublishEventCount)
  }

  override def processElement(data: MigrationMetadata, context: ProcessFunction[MigrationMetadata, String]#Context, metrics: Metrics): Unit = {
    logger.info("QuestionSet Migration started for : " + data.identifier)
    metrics.incCounter(config.questionSetMigrationEventCount)
    val definition: ObjectDefinition = definitionCache.getDefinition(data.objectType, data.schemaVersion, config.definitionBasePath)
    val readerConfig = ExtDataConfig(config.questionSetKeyspaceName, config.questionSetTableName, definition.getExternalPrimaryKey, definition.getExternalProps)
    val objData = getObject(data.identifier, readerConfig)(neo4JUtil, cassandraUtil, config)
    val messages: List[String] = validateQuestionSet(data.identifier, objData)(neo4JUtil)
    if (messages.isEmpty) {
      val migratedObj: ObjectData = migrateQuestionSet(objData)(definition, neo4JUtil).getOrElse(objData)
      val status = migratedObj.metadata.getOrElse("status", "").asInstanceOf[String]
      val qumlVersion: Double = migratedObj.metadata.getOrElse("qumlVersion", 1.0).asInstanceOf[Double]
      val migrationVersion: Double = migratedObj.metadata.getOrElse("migrationVersion", 0.0).asInstanceOf[Double]
      if (migrationVersion == 3.0 && qumlVersion == 1.1) {
        val upgradedQumlDef: ObjectDefinition = definitionCache.getDefinition(data.objectType, qumlVersion.toString, config.definitionBasePath)
        val qumlReaderConfig = ExtDataConfig(config.questionSetKeyspaceName, upgradedQumlDef.getExternalTable, upgradedQumlDef.getExternalPrimaryKey, upgradedQumlDef.getExternalProps)
        saveOnSuccess(migratedObj)(neo4JUtil, cassandraUtil, qumlReaderConfig, definitionCache, config)
        metrics.incCounter(config.questionSetMigrationSuccessEventCount)
        logger.info("QuestionSet Migration Successful For : " + data.identifier)
        if (statusList.contains(status)) {
          pushQuestionPublishEvent(migratedObj.metadata, context, metrics, config)
          logger.info("QuestionSet Re Publish Event Triggered Successfully For : " + data.identifier)
        }
      } else {
        logger.info("QuestionSet Migration Failed For : " + data.identifier + " | Errors : " + messages.mkString("; "))
        val errorMessages = messages.mkString("; ")
        val metadata = objData.metadata ++ Map[String, AnyRef]("migrationVersion" -> 2.1.asInstanceOf[AnyRef], "migrationError" -> errorMessages)
        val newObj = new ObjectData(objData.identifier, metadata, objData.extData, objData.hierarchy)
        saveOnFailure(newObj)(neo4JUtil)
        metrics.incCounter(config.questionSetMigrationFailedEventCount)
      }
    } else {
      val errorMessages = messages.mkString("; ")
      val metadata = objData.metadata ++ Map[String, AnyRef]("migrationVersion" -> 2.3.asInstanceOf[AnyRef], "migrationError" -> errorMessages)
      val newObj = new ObjectData(objData.identifier, metadata, objData.extData, objData.hierarchy)
      logger.info("QuestionSet Migration Skipped For : " + data.identifier + " | Errors : "+ messages.mkString("; "))
      saveOnFailure(newObj)(neo4JUtil)
      metrics.incCounter(config.questionSetMigrationSkippedEventCount)
    }
  }

  def pushQuestionPublishEvent(objMetadata: Map[String, AnyRef], context: ProcessFunction[MigrationMetadata, String]#Context, metrics: Metrics, config: QumlMigratorConfig): Unit = {
    context.output(config.liveQuestionSetPublishEventOutTag, getRepublishEvent(objMetadata, "question-republish", config.jobEnv))
    metrics.incCounter(config.questionSetRepublishEventCount)
  }

}
