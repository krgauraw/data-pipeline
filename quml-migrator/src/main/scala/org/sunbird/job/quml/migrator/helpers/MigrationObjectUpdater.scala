package org.sunbird.job.quml.migrator.helpers

import org.neo4j.driver.v1.StatementResult
import org.slf4j.LoggerFactory
import org.sunbird.job.domain.`object`.DefinitionCache
import org.sunbird.job.quml.migrator.domain.{ExtDataConfig, ObjectData}
import org.sunbird.job.quml.migrator.task.QumlMigratorConfig
import org.sunbird.job.util.{CassandraUtil, JSONUtil, Neo4JUtil, ScalaJsonUtil}
import java.util

trait MigrationObjectUpdater {

  private[this] val logger = LoggerFactory.getLogger(classOf[MigrationObjectUpdater])

  @throws[Exception]
  def saveOnSuccess(obj: ObjectData)(implicit neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil, readerConfig: ExtDataConfig, definitionCache: DefinitionCache, config: QumlMigratorConfig): Unit = {
    val identifier = obj.identifier
    val metadataUpdateQuery = metaDataQuery(obj)(definitionCache, config)
    val query = s"""MATCH (n:domain{IL_UNIQUE_ID:"$identifier"}) SET $metadataUpdateQuery;"""
    logger.info("MigrationObjectUpdater:: saveOnSuccess:: Query: " + query)
    val result: StatementResult = neo4JUtil.executeQuery(query)
    if (null != result && result.hasNext)
      logger.info(s"MigrationObjectUpdater:: saveOnSuccess:: statement result : ${result.next().asMap()}")
    saveExternalData(obj, readerConfig)
  }

  def saveExternalData(obj: ObjectData, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Unit


  @throws[Exception]
  def saveOnFailure(obj: ObjectData)(implicit neo4JUtil: Neo4JUtil): Unit = {
    val nodeId = obj.dbId
    val migrationError = obj.metadata.getOrElse("migrationError", "").asInstanceOf[String]
    val query = s"""MATCH (n:domain{IL_UNIQUE_ID:"$nodeId"}) SET n.migrationVersion=${obj.migrationVersion},n.migrationError="$migrationError";"""
    logger.info("MigrationObjectUpdater:: saveOnFailure:: Query: " + query)
    neo4JUtil.executeQuery(query)
  }

  def metaDataQuery(obj: ObjectData)(definitionCache: DefinitionCache, config: QumlMigratorConfig): String = {
    val scVer = obj.metadata.getOrElse("schemaVersion", "1.0").asInstanceOf[String]
    val definition = definitionCache.getDefinition(obj.dbObjType, scVer, config.definitionBasePath)
    val metadata = obj.metadata - ("IL_UNIQUE_ID", "identifier", "IL_FUNC_OBJECT_TYPE", "IL_SYS_NODE_TYPE", "pkgVersion", "lastStatusChangedOn", "lastUpdatedOn", "status", "objectType", "publish_type")
    metadata.map(prop => {
      if (null == prop._2) s"n.${prop._1}=${prop._2}"
      else if (definition.objectTypeProperties.contains(prop._1)) {
        prop._2 match {
          case _: Map[String, AnyRef] =>
            val strValue = JSONUtil.serialize(ScalaJsonUtil.serialize(prop._2))
            s"""n.${prop._1}=$strValue"""
          case _: util.Map[String, AnyRef] =>
            val strValue = JSONUtil.serialize(JSONUtil.serialize(prop._2))
            s"""n.${prop._1}=$strValue"""
          case _ =>
            val strValue = JSONUtil.serialize(prop._2)
            s"""n.${prop._1}=$strValue"""
        }
      } else {
        prop._2 match {
          case _: Map[String, AnyRef] =>
            val strValue = JSONUtil.serialize(ScalaJsonUtil.serialize(prop._2))
            s"""n.${prop._1}=$strValue"""
          case _: util.Map[String, AnyRef] =>
            val strValue = JSONUtil.serialize(JSONUtil.serialize(prop._2))
            s"""n.${prop._1}=$strValue"""
          case _: List[String] =>
            val strValue = ScalaJsonUtil.serialize(prop._2)
            s"""n.${prop._1}=$strValue"""
          case _: util.List[String] =>
            val strValue = JSONUtil.serialize(prop._2)
            s"""n.${prop._1}=$strValue"""
          case _ =>
            val strValue = JSONUtil.serialize(prop._2)
            s"""n.${prop._1}=$strValue"""
        }
      }
    }).mkString(",")
  }

}
