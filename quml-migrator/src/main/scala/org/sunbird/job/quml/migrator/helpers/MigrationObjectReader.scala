package org.sunbird.job.quml.migrator.helpers

import org.slf4j.LoggerFactory
import org.sunbird.job.quml.migrator.domain.{ExtDataConfig, ObjectData, ObjectExtData}
import org.sunbird.job.quml.migrator.task.QumlMigratorConfig
import org.sunbird.job.util.{CassandraUtil, Neo4JUtil}


import scala.collection.JavaConverters._

trait MigrationObjectReader {

  private[this] val logger = LoggerFactory.getLogger(classOf[MigrationObjectReader])

  def getObject(identifier: String, readerConfig: ExtDataConfig)(implicit neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil, config: QumlMigratorConfig): ObjectData = {
    logger.info("Reading object data for: " + identifier)
    val metadata = getMetadata(identifier)
    logger.info("Reading metadata for: " + identifier + " with metadata: " + metadata)
    val extData = getExtData(identifier, readerConfig)
    logger.info("Reading extData for: " + identifier + " with extData: " + extData)
    new ObjectData(identifier, metadata, extData.getOrElse(ObjectExtData()).data, extData.getOrElse(ObjectExtData()).hierarchy)
  }

  def getMetadata(identifier: String)(implicit neo4JUtil: Neo4JUtil): Map[String, AnyRef] = {
    val metaData = neo4JUtil.getNodeProperties(identifier).asScala.toMap
    val id = metaData.getOrElse("IL_UNIQUE_ID", identifier).asInstanceOf[String]
    val objType = metaData.getOrElse("IL_FUNC_OBJECT_TYPE", "").asInstanceOf[String]
    logger.info("MigrationObjectReader:: getMetadata:: identifier: " + identifier + " with objType: " + objType)
    metaData ++ Map[String, AnyRef]("identifier" -> id, "objectType" -> objType) - ("IL_UNIQUE_ID", "IL_FUNC_OBJECT_TYPE", "IL_SYS_NODE_TYPE")
  }

  def getExtData(identifier: String, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil, config: QumlMigratorConfig): Option[ObjectExtData]

}
