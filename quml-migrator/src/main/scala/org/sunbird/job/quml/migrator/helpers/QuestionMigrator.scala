package org.sunbird.job.quml.migrator.helpers

import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.{Clause, QueryBuilder, Select}
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.domain.`object`.ObjectDefinition
import org.sunbird.job.quml.migrator.domain.{ExtDataConfig, ObjectData, ObjectExtData}
import org.sunbird.job.quml.migrator.exceptions.QumlMigrationException
import org.sunbird.job.quml.migrator.task.QumlMigratorConfig
import org.sunbird.job.util._

import scala.collection.JavaConverters._
import java.util
import scala.collection.mutable.ListBuffer

trait QuestionMigrator extends MigrationObjectReader with MigrationObjectUpdater with QumlMigrator {

  private[this] val logger = LoggerFactory.getLogger(classOf[QuestionMigrator])

  override def getExtData(identifier: String, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil, config: QumlMigratorConfig): Option[ObjectExtData] = {
    val row: Row = getQuestionData(identifier, readerConfig)
    val data = if (null != row) Option(readerConfig.propsMapping.keySet.map(prop => prop -> row.getString(prop.toLowerCase())).toMap.filter(p => StringUtils.isNotBlank(p._2))) else Option(Map[String, AnyRef]())
    Option(ObjectExtData(data))
  }

  def getQuestionData(identifier: String, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Row = {
    logger.info("QuestionMigrator ::: getQuestionData ::: Reading Question External Data For : " + identifier)
    val select = QueryBuilder.select()
    val extProps: Set[String] = readerConfig.propsMapping.keySet
    if (null != extProps && !extProps.isEmpty) {
      extProps.foreach(prop => {
        if ("blob".equalsIgnoreCase(readerConfig.propsMapping.getOrElse(prop, "").asInstanceOf[String]))
          select.fcall("blobAsText", QueryBuilder.column(prop)).as(prop)
        else
          select.column(prop).as(prop)
      })
    }
    val selectWhere: Select.Where = select.from(readerConfig.keyspace, readerConfig.table).where().and(QueryBuilder.eq(readerConfig.primaryKey.head, identifier))
    logger.info("Cassandra Fetch Query :: " + selectWhere.toString)
    cassandraUtil.findOne(selectWhere.toString)
  }

  override def saveExternalData(obj: ObjectData, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Unit = {
    val extData = obj.extData.getOrElse(Map())
    val identifier = obj.identifier
    val columns = readerConfig.propsMapping.keySet
    val query = QueryBuilder.update(readerConfig.keyspace, readerConfig.table)
    val clause: Clause = QueryBuilder.eq(readerConfig.primaryKey.head, identifier)
    query.where.and(clause)
    columns.foreach(col => {
      readerConfig.propsMapping.getOrElse(col, "").asInstanceOf[String].toLowerCase match {
        case "blob" => extData.getOrElse(col, "") match {
          case value: String => query.`with`(QueryBuilder.set(col, QueryBuilder.fcall("textAsBlob", extData.getOrElse(col, ""))))
          case _ => query.`with`(QueryBuilder.set(col, QueryBuilder.fcall("textAsBlob", JSONUtil.serialize(extData.getOrElse(col, "")))))
        }
        case "string" => extData.getOrElse(col, "") match {
          case value: String => query.`with`(QueryBuilder.set(col, extData.getOrElse(col, null)))
          case _ => query.`with`(QueryBuilder.set(col, JSONUtil.serialize(extData.getOrElse(col, ""))))
        }
        case _ => query.`with`(QueryBuilder.set(col, extData.getOrElse(col, null)))
      }
    })

    logger.info(s"Updating Question in Cassandra For $identifier : ${query.toString}")
    val result = cassandraUtil.upsert(query.toString)
    if (result) {
      logger.info(s"Question Updated Successfully For $identifier")
    } else {
      val msg = s"Question Update Failed For $identifier"
      logger.error(msg)
      throw new Exception(msg)
    }
  }

  override def migrateQuestion(data: ObjectData)(implicit definition: ObjectDefinition): Option[ObjectData] = {
    logger.info("QuestionMigrator ::: migrateQuestion ::: Stating Data Transformation For : " + data.identifier)
    try {
      val jMap: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]()
      jMap.putAll(data.metadata.asJava)
      val extMeta: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]()
      val propsMapping: Map[String, String] = definition.getPropsType(definition.externalProperties)
      data.extData.getOrElse(Map[String, AnyRef]()).foreach(x => {
        if (List("body", "answer").contains(x._1))
          extMeta.put(x._1, x._2)
        else
          extMeta.put(x._1, getFormatedData(x._2.asInstanceOf[String], propsMapping.getOrElse(x._1, "")))
      })
      extMeta.put("primaryCategory", jMap.getOrDefault("primaryCategory", "").asInstanceOf[String])
      val migratedExtData = migrateExtData(data.identifier, extMeta)
      migratedExtData.remove("primaryCategory")
      val migrGrpahData: util.Map[String, AnyRef] = migrateGrpahData(data.identifier, jMap)
      migrGrpahData.put("bloomsLevel", null)
      migrGrpahData.put("version", null)
      val updatedMeta: Map[String, AnyRef] = migrGrpahData.asScala.toMap ++ Map[String, AnyRef]("qumlVersion" -> 1.1.asInstanceOf[AnyRef], "schemaVersion" -> "1.1", "migrationVersion" -> 3.0.asInstanceOf[AnyRef])
      logger.info("QuestionMigrator ::: migrateQuestion ::: migrated metadata :::: "+migrGrpahData)
      logger.info("QuestionMigrator ::: migrateQuestion ::: migrated ext data :::: "+migratedExtData)
      logger.info("QuestionMigrator ::: migrateQuestion ::: Completed Data Transformation For : " + data.identifier)
      Some(new ObjectData(data.identifier, updatedMeta, Some(migratedExtData.asScala.toMap), data.hierarchy))
    } catch {
      case e: java.lang.Exception => {
        logger.info("QuestionMigrator ::: migrateQuestion ::: Failed Data Transformation For : " + data.identifier)
        logger.info("QuestionMigrator ::: migrateQuestion ::: exception message :: "+ e.getMessage)
        logger.info("QuestionMigrator ::: migrateQuestion ::: exception message :: "+ e.getLocalizedMessage)
        e.printStackTrace()
        val updatedMeta: Map[String, AnyRef] = data.metadata ++ Map[String, AnyRef]("migrationVersion" -> 2.1.asInstanceOf[AnyRef], "migrationError"->e.getMessage)
        Some(new ObjectData(data.identifier, updatedMeta, data.extData, data.hierarchy))
      }
    }
  }

  def migrateGrpahData(identifier: String, data: util.Map[String, AnyRef]): util.Map[String, AnyRef] = {
    try {
      if (!data.isEmpty) {
        processBloomsLevel(data)
        processBooleanProps(data)
        data
      } else data
    } catch {
      case e: Exception => {
        logger.info(s"QuestionMigrator ::: migrateGrpahData ::: Error occurred while converting graph data for ${identifier} | Error: " +e.getMessage)
        e.printStackTrace()
        throw new QumlMigrationException(s"Error Occurred While Converting Graph Data To Quml 1.1 Format for ${identifier} | Error: "+e.getMessage)
      }
    }
  }

  def migrateExtData(identifier: String, data: util.Map[String, AnyRef]): util.Map[String, AnyRef] = {
    try {
      if (!data.isEmpty) {
        processResponseDeclaration(data)
        processInteractions(data)
        processSolutions(data)
        processInstructions(data)
        processHints(data)
        val ans = getAnswer(data)
        if (StringUtils.isNotBlank(ans))
          data.put("answer", ans)
        data
      } else data
    } catch {
      case e: java.lang.Exception => {
        e.printStackTrace()
        logger.info(s"QuestionMigrator ::: migrateExtData ::: Error occurred while converting external data for ${identifier} | Error: " + e.getMessage )
        throw new QumlMigrationException(s"Error Occurred While Converting External Data To Quml 1.1 Format for ${identifier} | Error: "+e.getMessage)
      }
    }
  }

  override def migrateQuestionSet(data: ObjectData)(implicit definition: ObjectDefinition, neo4JUtil: Neo4JUtil): Option[ObjectData] = None

  def getFormatedData(data: String, dType: String): AnyRef = {
    logger.info("getFormatedData ::: data ::: " + data)
    val value = dType match {
      case "object" => mapper.readValue(data, classOf[util.Map[String, AnyRef]])
      case "array" => mapper.readValue(data, classOf[util.List[util.Map[String, AnyRef]]])
      case _ => data
    }
    logger.info(s"getFormatedData ::: dType ::: ${dType} :::: formated value ::: " + value)
    return value
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

}
