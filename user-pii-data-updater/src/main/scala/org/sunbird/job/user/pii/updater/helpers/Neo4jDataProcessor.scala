package org.sunbird.job.user.pii.updater.helpers

import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.user.pii.updater.domain.ObjectData
import org.sunbird.job.util.Neo4JUtil

import scala.collection.JavaConverters._

trait Neo4jDataProcessor {

  private[this] val logger = LoggerFactory.getLogger(classOf[Neo4jDataProcessor])

  def searchObjects(objectType: String, lookupKey: String, schemaVersion: String, userId: String, targetKeys: List[String])(implicit neo4JUtil: Neo4JUtil): List[ObjectData] = {
    logger.info(s"Neo4jDataProcessor ::: searchObjects ::: Searching objects data for: objectType: ${objectType} , schemaVersion: ${schemaVersion}, userId: ${userId}, lookupKey: ${lookupKey}, targetKeys: ${targetKeys.asJava}")
    val output = generateSearchQuery(objectType, lookupKey, schemaVersion, userId, targetKeys)
    logger.info("neo4j graph search query :::: " + output._1)
    val statementResult = neo4JUtil.executeQuery(output._1)
    val metaList = List("identifier", "objectType") ++ output._2
    if (null != statementResult) {
      statementResult.list().asScala.toList.map(record => {
        val metadata: Map[String, AnyRef] = metaList.flatMap(meta => Map(meta -> record.get(meta).asString())).toMap
        new ObjectData(record.get("identifier").asString(), metadata)
      })
    } else {
      logger.info("No record found for objectType: ${objectType} , schemaVersion: ${schemaVersion}, lookupKey: ${lookupKey}, userId: ${userId}")
      List()
    }
  }

  def generateSearchQuery(objectType: String, lookupKey: String, schemaVersion: String, userId: String, targetKeys: List[String]): (String, List[String]) = {
    val graphQuery = s"""MATCH(n:domain) WHERE n.IL_FUNC_OBJECT_TYPE IN ["${objectType}","${objectType}Image"] AND n.IL_SYS_NODE_TYPE="DATA_NODE" AND n.${lookupKey}="${userId}" SC_VERSION RETURN n.IL_UNIQUE_ID AS identifier, n.IL_FUNC_OBJECT_TYPE AS objectType TARGET_FIELDS;"""
    val updatedTargetKeys: List[String] = targetKeys.map(key => {
      if (StringUtils.contains(key, ".")) {
        key.split("\\.")(0)
      } else key
    })
    logger.info("target keys to fetch ::: " + updatedTargetKeys)
    val props = List("status", "visibility") ++ updatedTargetKeys
    val buffer = new StringBuilder
    props.foreach(x => {
      buffer.append(s""",n.${x} as ${x}""")
    })
    val updatedQuery = if (StringUtils.equals("1.1", schemaVersion)) {
      graphQuery.replace("SC_VERSION", """ AND n.schemaVersion="1.1"""")
    } else graphQuery.replace("SC_VERSION", """ AND n.schemaVersion IS null""")
    val query = updatedQuery.replace("TARGET_FIELDS", buffer)
    (query, props)
  }

  def updateObject(identifier: String, metadata: Map[String, AnyRef])(implicit neo4JUtil: Neo4JUtil): String = {
    val updatedId = try {
      if (!metadata.isEmpty) {
        val metaQuery = metadata.map(x => {
          if (null != x)
            s"""n.${x._1} = "${x._2}""""
        }).mkString(",")
        val graphQuery = s"""match(n:domain) where n.IL_UNIQUE_ID="${identifier}" set ${metaQuery};"""
        logger.info(s"Neo4jDataProcessor ::: updateObject ::: graph query for update : ${graphQuery}")
        val statementResult = neo4JUtil.executeQuery(graphQuery)
        if (null != statementResult) identifier else ""
      } else ""
    } catch {
      case e: Exception => ""
    }
    updatedId
  }


}
