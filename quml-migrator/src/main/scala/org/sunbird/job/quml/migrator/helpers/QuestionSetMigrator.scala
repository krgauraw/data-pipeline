package org.sunbird.job.quml.migrator.helpers

import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.{Clause, Insert, QueryBuilder}
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.domain.`object`.ObjectDefinition
import org.sunbird.job.quml.migrator.domain.{ExtDataConfig, ObjectData, ObjectExtData}
import org.sunbird.job.quml.migrator.exceptions.QumlMigrationException
import org.sunbird.job.quml.migrator.task.QumlMigratorConfig
import org.sunbird.job.util.{CassandraUtil, Neo4JUtil, ScalaJsonUtil}

import java.util
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

trait QuestionSetMigrator extends MigrationObjectReader with MigrationObjectUpdater with QumlMigrator {

	private[this] val logger = LoggerFactory.getLogger(classOf[QuestionSetMigrator])

	val propsToRemove = List("outcomeDeclaration", "bloomsLevel", "maxScore","version")

	def validateQuestionSet(identifier: String, obj: ObjectData)(implicit neo4JUtil: Neo4JUtil): List[String] = {
		val messages = ListBuffer[String]()
		if (obj.hierarchy.getOrElse(Map()).isEmpty) messages += s"""There is no hierarchy available for : $identifier"""
		if (!StringUtils.equalsIgnoreCase(obj.getString("mimeType", ""), "application/vnd.sunbird.questionset"))
			messages += s"""mimeType is invalid for : $identifier"""
		if (obj.getString("visibility", "").isEmpty) messages += s"""There is no visibility available for : $identifier"""
		val childIds = getChildrenIdentifiers(obj)
		childIds.foreach(id => {
			val metadata = getMetadata(id)
			val schemaVersion = metadata.getOrElse("schemaVersion", "0.0").asInstanceOf[String]
			val migrationVersion: Double = metadata.getOrElse("migrationVersion", 0.0).asInstanceOf[Double]
			val migrVer = List[Double](3.0, 3.1)
			if(!(StringUtils.equalsIgnoreCase("1.1", schemaVersion) && migrVer.contains(migrationVersion)))
				messages += s"""Please migrate children having identifier ${id} first."""
		})
		messages.toList
	}

	override def getExtData(identifier: String,  readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil, config: QumlMigratorConfig): Option[ObjectExtData] = {
		val row: Row = getQuestionSetData(identifier, readerConfig)
		val data: Map[String, AnyRef] = if (null != row) readerConfig.propsMapping.keySet.map(prop => prop -> row.getString(prop.toLowerCase())).toMap.filter(p => StringUtils.isNotBlank(p._2.asInstanceOf[String])) else Map[String, AnyRef]()
		val hData: String = data.getOrElse("hierarchy", "{}").asInstanceOf[String]
		val hierarchy: Map[String, AnyRef] = if (data.contains("hierarchy")) ScalaJsonUtil.deserialize[Map[String, AnyRef]](hData) else Map[String, AnyRef]()
		val extData: Map[String, AnyRef] = data.filter(p => !StringUtils.equals("hierarchy", p._1))
		Option(ObjectExtData(Option(extData), Option(hierarchy)))
	}

	def getQuestionSetData(identifier: String, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Row = {
		logger.info("QuestionSetMigrator ::: getQuestionSetData ::: Reading QuestionSet External Data For : " + identifier)
		val qsExtProps = readerConfig.propsMapping.keySet
		val select = QueryBuilder.select()
		select.column(readerConfig.primaryKey(0)).as(readerConfig.primaryKey(0))
		if (null != qsExtProps && !qsExtProps.isEmpty) {
			qsExtProps.foreach(prop => {
				if ("blob".equalsIgnoreCase(readerConfig.propsMapping.getOrElse(prop, "").asInstanceOf[String]))
					select.fcall("blobAsText", QueryBuilder.column(prop)).as(prop)
				else
					select.column(prop).as(prop)
			})
		}
		val selectQuery = select.from(readerConfig.keyspace, readerConfig.table)
		val clause: Clause = QueryBuilder.eq("identifier", identifier)
		selectQuery.where.and(clause)
		logger.info("Cassandra Fetch Query :: " + selectQuery.toString)
		cassandraUtil.findOne(selectQuery.toString)
	}

	def getChildrenIdentifiers(qsObj: ObjectData): List[String] = {
		val childrenMaps: Map[String, AnyRef] = populateChildrenMapRecursively(qsObj.hierarchy.getOrElse(Map()).getOrElse("children", List()).asInstanceOf[List[Map[String, AnyRef]]], Map())
		childrenMaps.keys.toList
	}

	def populateChildrenMapRecursively(children: List[Map[String, AnyRef]], childrenMap: Map[String, AnyRef]): Map[String, AnyRef] = {
		val result = children.flatMap(child => {
			val updatedChildrenMap: Map[String, AnyRef] =
				if (child.getOrElse("objectType", "").asInstanceOf[String].equalsIgnoreCase("Question")) {
					Map(child.getOrElse("identifier", "").asInstanceOf[String] -> child) ++ childrenMap
				} else childrenMap
			val nextChild: List[Map[String, AnyRef]] = child.getOrElse("children", List()).asInstanceOf[List[Map[String, AnyRef]]]
			val map = populateChildrenMapRecursively(nextChild, updatedChildrenMap)
			map ++ updatedChildrenMap
		}).toMap
		result
	}

	override def migrateQuestionSet(data: ObjectData)(implicit definition: ObjectDefinition, neo4JUtil: Neo4JUtil): Option[ObjectData] = {
		logger.info("QuestionSetMigrator ::: migrateQuestionSet ::: Stating Data Transformation For : " + data.identifier)
		try {
			val metaMap: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]()
			metaMap.putAll(data.metadata.asJava)
			val extMeta: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]()
			if(data.extData.getOrElse(Map[String, AnyRef]()).contains("instructions")) {
				val instructions = data.extData.getOrElse(Map[String, AnyRef]()).asJava.getOrElse("instructions", "{}").asInstanceOf[String]
				extMeta.put("instructions", mapper.readValue(instructions, classOf[util.Map[String, AnyRef]]))
			}
			val hStr: String = ScalaJsonUtil.serialize(data.hierarchy.getOrElse(Map[String, AnyRef]()))
			val hierarchyData = mapper.readValue(hStr, classOf[util.Map[String, AnyRef]])
			val migrGrpahData: util.Map[String, AnyRef] = migrateGrpahData(data.identifier, metaMap)
			val migrExtData: util.Map[String, AnyRef] = migrateExtData(data.identifier, extMeta)
			val outcomeDeclaration: util.Map[String, AnyRef] = migrGrpahData.getOrDefault("outcomeDeclaration", Map[String, AnyRef]()).asInstanceOf[util.Map[String, AnyRef]]
			propsToRemove.foreach(prop => migrGrpahData.put(prop, null))
			migrExtData.put("outcomeDeclaration", outcomeDeclaration)
			val migrHierarchy: util.Map[String, AnyRef] = migrateHierarchy(data.identifier, hierarchyData)
			logger.info("migrateQuestionSet :: migrated graph data ::: " + migrGrpahData)
			logger.info("migrateQuestionSet :: migrated ext data ::: " + migrExtData)
			logger.info("migrateQuestionSet :: migrated hierarchy ::: " + migrHierarchy)
			val updatedMeta: Map[String, AnyRef] = migrGrpahData.asScala.toMap ++ Map[String, AnyRef]("qumlVersion" -> 1.1.asInstanceOf[AnyRef], "schemaVersion" -> "1.1", "migrationVersion" -> 3.0.asInstanceOf[AnyRef])
			logger.info("QuestionSetMigrator ::: migrateQuestionSet ::: Completed Data Transformation For : " + data.identifier)
			Some(new ObjectData(data.identifier, updatedMeta, Some(migrExtData.asScala.toMap), Some(migrHierarchy.asScala.toMap)))
		} catch {
			case e: java.lang.Exception => {
				logger.info("QuestionSetMigrator ::: migrateQuestionSet ::: Failed Data Transformation For : " + data.identifier)
				logger.info("QuestionSetMigrator ::: migrateQuestionSet ::: exception message :: "+ e.getMessage)
				logger.info("QuestionSetMigrator ::: migrateQuestionSet ::: exception message :: "+ e.getLocalizedMessage)
				val updatedMeta: Map[String, AnyRef] = data.metadata ++ Map[String, AnyRef]("migrationVersion" -> 2.1.asInstanceOf[AnyRef], "migrationError"->e.getMessage)
				Some(new ObjectData(data.identifier, updatedMeta, data.extData, data.hierarchy))
			}
		}
	}

	def migrateGrpahData(identifier: String, data: util.Map[String, AnyRef]): util.Map[String, AnyRef] = {
		try {
			if (!data.isEmpty) {
				processMaxScore(data)
				processBloomsLevel(data)
				processBooleanProps(data)
				processTimeLimits(data)
				data
			} else data
		} catch {
			case e: java.lang.Exception => {
				e.printStackTrace()
				logger.info(s"QuestionSetMigrator  ::  migrateGrpahData ::: Error Occurred While Graph Data Transformation For ${identifier} | Error: "+ e.getMessage)
				throw new QumlMigrationException(s"Error Occurred While Converting Graph Data To Quml 1.1 Format for ${identifier} | Error: "+e.getMessage)
			}
		}
	}

	def migrateExtData(identifier: String, data: util.Map[String, AnyRef]): util.Map[String, AnyRef] = {
		try {
			if (!data.isEmpty) {
				processInstructions(data)
				data
			} else data
		} catch {
			case e: java.lang.Exception => {
				e.printStackTrace()
				logger.info(s"QuestionSetMigrator  ::  migrateExtData ::: Error Occurred While External Data Transformation For ${identifier} | Error: "+ e.getMessage)
				throw new QumlMigrationException(s"Error Occurred While Converting External Data To Quml 1.1 Format for ${identifier} | Error : "+e.getMessage)
			}
		}
	}

	def migrateHierarchy(identifier: String, data: util.Map[String, AnyRef])(implicit neo4JUtil: Neo4JUtil): util.Map[String, AnyRef] = {
		try {
			if (!data.isEmpty) {
				logger.info(s"QuestionSetMigrator ::: migrateHierarchy ::: Hierarchy migration stated for ${identifier}")
				if(data.containsKey("maxScore")) data.remove("maxScore")
				data.remove("version")
				processInstructions(data)
				processBloomsLevel(data)
				processBooleanProps(data)
				processTimeLimits(data)
				val status = data.getOrDefault("status","").asInstanceOf[String]
				val liveStatus = List("Live", "Unlisted")
				if(StringUtils.isNotBlank(status) && liveStatus.contains(status))
					data.putAll(Map("qumlVersion" -> 1.1.asInstanceOf[AnyRef], "schemaVersion" -> "1.1", "migrationVersion" -> 3.0.asInstanceOf[AnyRef]).asJava)
				val children = data.getOrDefault("children", new util.ArrayList[java.util.Map[String, AnyRef]]).asInstanceOf[util.List[java.util.Map[String, AnyRef]]]
				if (!children.isEmpty)
					migrateChildren(children)
				data
			} else data
		} catch {
			case e: java.lang.Exception => {
				logger.info(s"QuestionSetMigrator  ::  migrateHierarchy ::: Error Occurred While Hierarchy Data Transformation For ${identifier} | Error: "+ e.getMessage)
				e.printStackTrace()
				throw new QumlMigrationException(s"Error Occurred While Converting Hierarchy Data To Quml 1.1 Format for ${identifier} | Error: "+e.getMessage)
			}
		}
	}

	def migrateChildren(children: util.List[util.Map[String, AnyRef]])(implicit neo4JUtil: Neo4JUtil): Unit = {
		if (!children.isEmpty) {
			children.foreach(ch => {
				if (StringUtils.equalsIgnoreCase("application/vnd.sunbird.questionset", ch.getOrDefault("mimeType", "").asInstanceOf[String])) {
					if (ch.containsKey("version")) ch.remove("version")
					processBloomsLevel(ch)
					processBooleanProps(ch)
					processTimeLimits(ch)
					processInstructions(ch)
					ch.put("schemaVersion", "1.1")
					ch.put("qumlVersion", 1.1.asInstanceOf[AnyRef])
					ch.put("migrationVersion", 3.0.asInstanceOf[AnyRef])
					val nestedChildren = ch.getOrDefault("children", new util.ArrayList[java.util.Map[String, AnyRef]]).asInstanceOf[util.List[java.util.Map[String, AnyRef]]]
					migrateChildren(nestedChildren)
				} else {
					val childrenId = ch.get("identifier").asInstanceOf[String].replace(".img","")
					val chData = getMetadata(childrenId)(neo4JUtil)
					val schemaVersion = chData.getOrElse("schemaVersion", "1.0").asInstanceOf[String]
					val migrVer:Double = chData.getOrElse("migrationVersion", 1.0.asInstanceOf[AnyRef]).asInstanceOf[Double]
					if(StringUtils.equalsIgnoreCase("1.1", schemaVersion) && List(3.0,3.1).contains(migrVer)) {
						val chStr = ScalaJsonUtil.serialize(chData)
						val chMap: util.Map[String, AnyRef] = mapper.readValue(chStr, classOf[util.Map[String, AnyRef]])
						ch.putAll(chMap)
						ch.remove("bloomsLevel")
						ch.remove("version")
					} else throw new QumlMigrationException(s"Please migrate children having identifier ${childrenId}")
				}
			})
		}
	}

	override def saveExternalData(obj: ObjectData, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil) = {
		val identifier = obj.identifier
		var data = Map[String, AnyRef]("hierarchy"-> obj.hierarchy.getOrElse(Map())) ++ obj.extData.getOrElse(Map())
		val query: Insert = QueryBuilder.insertInto(readerConfig.keyspace, readerConfig.table)
		query.value(readerConfig.primaryKey(0), identifier)
		data.map(d => {
			readerConfig.propsMapping.getOrElse(d._1, "") match {
				case "blob" => query.value(d._1.toLowerCase, QueryBuilder.fcall("textAsBlob", d._2))
				case "string" => d._2 match {
					case value: String => query.value(d._1.toLowerCase, value)
					case _ => query.value(d._1.toLowerCase, ScalaJsonUtil.serialize(d._2))
				}
				case _ => query.value(d._1, d._2)
			}
		})
		logger.info(s"Saving object external data for $identifier | Query : ${query.toString}")
		val result = cassandraUtil.upsert(query.toString, false)
		if (result) {
			logger.info(s"Object external data saved successfully for ${identifier}")
		} else {
			val msg = s"Object External Data Insertion Failed For ${identifier}"
			logger.error(msg)
			throw new Exception(msg)
		}
	}

	override def migrateQuestion(data: ObjectData)(implicit definition: ObjectDefinition): Option[ObjectData] = None
}
