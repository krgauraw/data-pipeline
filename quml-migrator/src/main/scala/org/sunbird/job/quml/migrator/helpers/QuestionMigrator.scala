package org.sunbird.job.quml.migrator.helpers

import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.{Clause, QueryBuilder, Select}
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.quml.migrator.domain.{ExtDataConfig, ObjectData, ObjectExtData}
import org.sunbird.job.quml.migrator.task.QumlMigratorConfig
import org.sunbird.job.util._
import scala.collection.JavaConverters._
import java.util

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

  override def migrateQuestion(data: ObjectData): Option[ObjectData] = {
    logger.info("QuestionMigrator ::: migrateQuestion ::: Stating Data Transformation For : " + data.identifier)
    try {
      val jMeta = data.metadata.asJava
      logger.info("jMeta ::: "+jMeta)
      logger.info("type check ::: "+jMeta.isInstanceOf[util.Map[String, AnyRef]])
      logger.info("type check hash::: "+jMeta.isInstanceOf[util.HashMap[String, AnyRef]])

      val newMap: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]()
      newMap.putAll(jMeta)
      logger.info("newMap ::: "+newMap)
      logger.info("newMap type check ::: "+newMap.isInstanceOf[util.HashMap[String, AnyRef]])
      val b = newMap.remove("bloomsLevel")
      logger.info("b :::: "+b)

      val serial = ScalaJsonUtil.serialize(data.metadata)
      logger.info("serial data ::: "+serial)
      val jMap = ScalaJsonUtil.deserialize[util.HashMap[String, AnyRef]](serial)
      logger.info("jMap ::: "+jMap)
      logger.info("jMap type :: "+jMap.isInstanceOf[util.HashMap[String, AnyRef]])
     /* val meta : util.HashMap[String, AnyRef] =  data.metadata.asJava.asInstanceOf[util.HashMap[String, AnyRef]]
      logger.info("meta ::: "+ meta)
      logger.info("meta type check ::: "+ meta.isInstanceOf[util.HashMap[String, AnyRef]])*/
      val migrGrpahData: Map[String, AnyRef] = migrateGrpahData(data.identifier, jMap).asScala.toMap
      val migrExtData: Map[String, AnyRef] = migrateExtData(data.identifier, data.extData.getOrElse(Map[String, AnyRef]()).asJava).asScala.toMap
      val updatedMeta: Map[String, AnyRef] = migrGrpahData ++ Map[String, AnyRef]("qumlVersion" -> 1.1.asInstanceOf[AnyRef], "schemaVersion" -> "1.1", "migrationVersion" -> 3.0.asInstanceOf[AnyRef])
      logger.info("QuestionMigrator ::: migrateQuestion ::: Completed Data Transformation For : " + data.identifier)
      Some(new ObjectData(data.identifier, updatedMeta, Some(migrExtData), data.hierarchy))
    } catch {
      case e: Throwable => {
        e.printStackTrace()
        logger.info("QuestionMigrator ::: migrateQuestion ::: Failed Data Transformation For : " + data.identifier)
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
      case e: Throwable => {
        e.printStackTrace()
        throw new Exception(s"Error Occurred While Converting Graph Data To Quml 1.1 Format for ${identifier}")
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
      case e: Throwable => {
        e.printStackTrace()
        throw new Exception(s"Error Occurred While Converting External Data To Quml 1.1 Format for ${identifier}")
      }
    }
  }

  override def migrateQuestionSet(data: ObjectData): Option[ObjectData] = None


}
