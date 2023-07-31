package org.sunbird.job.questionset.republish.helpers

import java.io.File

import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.{Clause, Insert, QueryBuilder, Select}
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.publish.config.PublishConfig
import org.sunbird.job.domain.`object`.{DefinitionCache, ObjectDefinition}
import org.sunbird.job.publish.core.{DefinitionConfig, ExtDataConfig, ObjectData, ObjectExtData}
import org.sunbird.job.publish.helpers._
import org.sunbird.job.util.{CSPMetaUtil, CassandraUtil, CloudStorageUtil, FileUtils, HttpUtil, JSONUtil, Neo4JUtil, ScalaJsonUtil, Slug}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.Duration

trait QuestionSetPublisher extends LiveObjectReader with ObjectValidator with ObjectUpdater with ObjectEnrichment with EcarGenerator with QuestionPdfGenerator {

	private[this] val logger = LoggerFactory.getLogger(classOf[QuestionSetPublisher])
	private val bundleLocation: String = "/tmp"
	private val indexFileName = "index.json"
	private val defaultManifestVersion = "1.2"

	override def getExternalData(identifier: String, pkgVersion: Double, mimeType: String, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil, config: PublishConfig): Option[ObjectExtData] = {
		val row: Row = getQuestionSetData(identifier, readerConfig)
		val data: Map[String, AnyRef] = if (null != row) readerConfig.propsMapping.keySet.map(prop => prop -> row.getString(prop.toLowerCase())).toMap.filter(p => StringUtils.isNotBlank(p._2.asInstanceOf[String])) else Map[String, AnyRef]()
		val hData: String = data.getOrElse("hierarchy", "{}").asInstanceOf[String]
		val updatedHierarchy = if(config.getBoolean("cloudstorage.metadata.replace_absolute_path", false)) CSPMetaUtil.updateAbsolutePath(hData) else hData
		val hierarchy: Map[String, AnyRef] = if(data.contains("hierarchy")) ScalaJsonUtil.deserialize[Map[String, AnyRef]](updatedHierarchy) else Map[String, AnyRef]()
		val extData:Map[String, AnyRef] = data.filter(p => !StringUtils.equals("hierarchy", p._1))
		Option(ObjectExtData(Option(extData), Option(hierarchy)))
	}

	def getQuestionSetData(identifier: String, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Row = {
		logger.info("QuestionSetPublisher ::: getQuestionSetData ::: Reading QuestionSet External Data For : "+identifier)
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
		logger.info("Cassandra Fetch Query :: "+ selectQuery.toString)
		cassandraUtil.findOne(selectQuery.toString)
	}

	def validateQuestionSet(obj: ObjectData, identifier: String): List[String] = {
		val messages = ListBuffer[String]()
		if (obj.hierarchy.getOrElse(Map()).isEmpty) messages += s"""There is no hierarchy available for : $identifier"""
		if (!StringUtils.equalsIgnoreCase(obj.getString("mimeType", ""), "application/vnd.sunbird.questionset"))
			messages += s"""mimeType is invalid for : $identifier"""
		if (obj.getString("visibility", "").isEmpty) messages += s"""There is no visibility available for : $identifier"""
		//TODO: Add any more check, if required.
		messages.toList
	}

	override def getHierarchy(identifier: String, pkgVersion: Double, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil, config: PublishConfig): Option[Map[String, AnyRef]] = {
		val row: Row = Option(getQuestionSetHierarchy(getEditableObjId(identifier, pkgVersion), readerConfig)).getOrElse(getQuestionSetHierarchy(identifier, readerConfig))
		if (null != row) {
			val hData: String = row.getString("hierarchy")
			val updatedHierarchy = if(config.getBoolean("cloudstorage.metadata.replace_absolute_path", false)) CSPMetaUtil.updateAbsolutePath(hData) else hData
			val hierarchy: Map[String, AnyRef] = if(StringUtils.isNotBlank(updatedHierarchy)) ScalaJsonUtil.deserialize[Map[String, AnyRef]](updatedHierarchy) else Map[String, AnyRef]()
			Option(hierarchy)
		} else Option(Map())
	}

	def getQuestionSetHierarchy(identifier: String, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil) = {
		val selectWhere: Select.Where = QueryBuilder.select().all()
		  .from(readerConfig.keyspace, readerConfig.table).
		  where()
		selectWhere.and(QueryBuilder.eq("identifier", identifier))
		cassandraUtil.findOne(selectWhere.toString)
	}

	override def getExtDatas(identifiers: List[String], readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = {
		val rows = getQuestionsExtData(identifiers, readerConfig)(cassandraUtil).asScala
		val extProps = readerConfig.propsMapping.keySet ++ Set("identifier")
		if (rows.nonEmpty)
			Option(rows.map(row => row.getString("identifier") -> extProps.map(prop => (prop -> row.getString(prop.toLowerCase()))).toMap).toMap)
		else
			Option(Map[String, AnyRef]())
	}

	override def getHierarchies(identifiers: List[String], readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil): Option[Map[String, AnyRef]] = {
		None
	}

	def getQuestionsExtData(identifiers: List[String], readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil) = {
		logger.info("QuestionSetPublisher ::: getQuestionsExtData ::: reader config ::: keyspace: " + readerConfig.keyspace + " ,  table : " + readerConfig.table)
		val select = QueryBuilder.select()
		val extProps: Set[String] = readerConfig.propsMapping.keySet ++ Set("identifier")
		if (null != extProps && !extProps.isEmpty) {
			extProps.foreach(prop => {
				if ("blob".equalsIgnoreCase(readerConfig.propsMapping.getOrElse(prop, "").asInstanceOf[String]))
					select.fcall("blobAsText", QueryBuilder.column(prop)).as(prop)
				else
					select.column(prop).as(prop)
			})
		}
		val selectWhere: Select.Where = select.from(readerConfig.keyspace, readerConfig.table).where()
		selectWhere.and(QueryBuilder.in("identifier", identifiers.asJava))
		logger.info("QuestionSetPublisher ::: getQuestionsExtData ::: cassandra query ::: " + selectWhere.toString)
		cassandraUtil.find(selectWhere.toString)
	}

	override def saveExternalData(obj: ObjectData, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil) = {
		val identifier = obj.identifier
		val children: List[Map[String, AnyRef]] = obj.hierarchy.getOrElse(Map()).getOrElse("children", List()).asInstanceOf[List[Map[String, AnyRef]]]
		val hierarchy: Map[String, AnyRef] = obj.metadata ++ Map("children" -> children)
		val data = Map("hierarchy" -> hierarchy) ++ obj.extData.getOrElse(Map())
		val query: Insert = QueryBuilder.insertInto(readerConfig.keyspace, readerConfig.table)
		query.value(readerConfig.primaryKey(0), identifier)
		data.map(d => {
			readerConfig.propsMapping.getOrElse(d._1, "") match {
				case "blob" => query.value(d._1.toLowerCase, QueryBuilder.fcall("textAsBlob", d._2))
				case "string" => d._2 match {
					case value: String => query.value(d._1.toLowerCase, value)
					case _ => query.value(d._1.toLowerCase, JSONUtil.serialize(d._2))
				}
				case _ => query.value(d._1, d._2)
			}
		})
		logger.debug(s"Saving object external data for $identifier | Query : ${query.toString}")
		val result = cassandraUtil.upsert(query.toString, true)
		if (result) {
			logger.info(s"Object external data saved successfully for ${identifier}")
		} else {
			val msg = s"Object External Data Insertion Failed For ${identifier}"
			logger.error(msg)
			throw new Exception(msg)
		}

	}

	def getQuestions(qsObj: ObjectData, readerConfig: ExtDataConfig)
	                (implicit cassandraUtil: CassandraUtil): List[ObjectData] = {
		val childrenMaps: Map[String, AnyRef] = populateChildrenMapRecursively(qsObj.hierarchy.getOrElse(Map()).getOrElse("children", List()).asInstanceOf[List[Map[String, AnyRef]]], Map())
		logger.info("QuestionSetPublisher ::: getQuestions ::: child questions  ::::: " + childrenMaps)
		val extMap = getExtDatas(childrenMaps.keys.toList, readerConfig)
		logger.info("QuestionSetPublisher ::: getQuestions ::: child questions external data ::::: " + extMap.get)
		extMap.getOrElse(Map()).map(entry => {
			val metadata = childrenMaps.getOrElse(entry._1, Map()).asInstanceOf[Map[String, AnyRef]] ++ Map("IL_UNIQUE_ID" -> entry._1)
			val obj = new ObjectData(entry._1, metadata, Option(entry._2.asInstanceOf[Map[String, AnyRef]]))
			obj
		}).toList
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

	override def getDataForEcar(obj: ObjectData): Option[List[Map[String, AnyRef]]] = {
		val hChildren: List[Map[String, AnyRef]] = obj.hierarchy.getOrElse(Map()).getOrElse("children", List()).asInstanceOf[List[Map[String, AnyRef]]]
		Some(getFlatStructure(List(obj.metadata ++ obj.extData.getOrElse(Map()) ++ Map("children" -> hChildren)), List()))
	}

	override def enrichObjectMetadata(obj: ObjectData)(implicit neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil, readerConfig: ExtDataConfig, cloudStorageUtil: CloudStorageUtil, config: PublishConfig, definitionCache: DefinitionCache, definitionConfig: DefinitionConfig): Option[ObjectData] = {
		val newMetadata: Map[String, AnyRef] = obj.metadata ++ Map("pkgVersion" -> (obj.pkgVersion + 1).asInstanceOf[AnyRef], "lastPublishedOn" -> getTimeStamp,
			"publishError" -> null, "variants" -> null, "downloadUrl" -> null, "status" -> "Live", "migrationVersion"->3.1.asInstanceOf[AnyRef])
		val children: List[Map[String, AnyRef]] = obj.hierarchy.getOrElse(Map()).getOrElse("children", List()).asInstanceOf[List[Map[String, AnyRef]]]
		Some(new ObjectData(obj.identifier, newMetadata, obj.extData, hierarchy = Some(Map("identifier" -> obj.identifier, "children" -> enrichChildren(children)))))
	}

	def enrichChildren(children: List[Map[String, AnyRef]])(implicit neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil, readerConfig: ExtDataConfig, definitionCache: DefinitionCache, definitionConfig: DefinitionConfig, config: PublishConfig): List[Map[String, AnyRef]] = {
		val newChildren = children.map(element => enrichMetadata(element))
		newChildren
	}

	def enrichMetadata(element: Map[String, AnyRef])(implicit neo4JUtil: Neo4JUtil, cassandraUtil: CassandraUtil, readerConfig: ExtDataConfig, definitionCache: DefinitionCache, definitionConfig: DefinitionConfig, config: PublishConfig): Map[String, AnyRef] = {
		if (StringUtils.equalsIgnoreCase(element.getOrElse("objectType", "").asInstanceOf[String], "QuestionSet")
		  && StringUtils.equalsIgnoreCase(element.getOrElse("visibility", "").asInstanceOf[String], "Parent")) {
			val children: List[Map[String, AnyRef]] = element.getOrElse("children", List()).asInstanceOf[List[Map[String, AnyRef]]]
			val enrichedChildren = enrichChildren(children)
			element ++ Map("children" -> enrichedChildren, "status" -> "Live", "migrationVersion"->3.1.asInstanceOf[AnyRef])
		} else if (StringUtils.equalsIgnoreCase(element.getOrElse("objectType", "").toString, "QuestionSet")
		  && StringUtils.equalsIgnoreCase(element.getOrElse("visibility", "").toString, "Default")) {
			val childHierarchy: Map[String, AnyRef] = getHierarchy(element.getOrElse("identifier", "").toString, 0.asInstanceOf[Double], readerConfig).getOrElse(Map())
			childHierarchy ++ Map("index" -> element.getOrElse("index", 0).asInstanceOf[AnyRef], "depth" -> element.getOrElse("depth", 0).asInstanceOf[AnyRef], "parent" -> element.getOrElse("parent", ""))
		} else if (StringUtils.equalsIgnoreCase(element.getOrElse("objectType", "").toString, "Question")) {
			val newObject: ObjectData = getObject(element.getOrElse("identifier", "").toString, 0.asInstanceOf[Double], element.getOrElse("mimeType", "").toString, element.getOrElse("publish_type", "Public").toString, readerConfig)
			if(newObject.metadata.getOrElse("migrationVersion", 0).asInstanceOf[AnyRef]!=3.1)
				throw new Exception(s"Children Having Identifier ${newObject.identifier} is not migrated.")
			val definition: ObjectDefinition = definitionCache.getDefinition("Question", definitionConfig.supportedVersion.getOrElse("question", "1.0").asInstanceOf[String], definitionConfig.basePath)
			val enMeta = newObject.metadata.filter(x => null != x._2).map(element => (element._1, convertJsonProperties(element, definition.getJsonProps())))
			logger.info("enrichMeta :::: question object meta ::: " + enMeta)
			enMeta ++ Map("index" -> element.getOrElse("index", 0).asInstanceOf[AnyRef], "parent" -> element.getOrElse("parent", ""), "depth" -> element.getOrElse("depth", 0).asInstanceOf[AnyRef])
		} else Map()
	}

	override def deleteExternalData(obj: ObjectData, readerConfig: ExtDataConfig)(implicit cassandraUtil: CassandraUtil) = None

	def updateArtifactUrl(obj: ObjectData, pkgType: String)(implicit ec: ExecutionContext, neo4JUtil: Neo4JUtil, cloudStorageUtil: CloudStorageUtil, defCache: DefinitionCache, defConfig: DefinitionConfig, config: PublishConfig, httpUtil: HttpUtil): ObjectData = {
		val bundlePath = bundleLocation + File.separator + obj.identifier + File.separator + System.currentTimeMillis + "_temp"
		try {
			val objType = obj.getString("objectType", "")
			val objList = getDataForEcar(obj).getOrElse(List())
			val (updatedObjList, dUrls) = getManifestData(obj.identifier, pkgType, objList)
			val downloadUrls: Map[AnyRef, List[String]] = dUrls.flatten.groupBy(_._1).map { case (k, v) => k -> v.map(_._2) }
			logger.info("QuestionPublisher ::: updateArtifactUrl ::: downloadUrls :::: " + downloadUrls)
			val duration: String = config.getString("media_download_duration", "300 seconds")
			val downloadedMedias: List[File] = Await.result(downloadFiles(obj.identifier, downloadUrls, bundlePath), Duration.apply(duration))
			if (downloadUrls.nonEmpty && downloadedMedias.isEmpty)
				throw new Exception("Error Occurred While Downloading Bundle Media Files For : " + obj.identifier)

			getIndexFile(obj.identifier, objType, bundlePath, updatedObjList)

			// create zip package
			val zipFileName: String = bundlePath + File.separator + obj.identifier + "_" + System.currentTimeMillis + ".zip"
			FileUtils.createZipPackage(bundlePath, zipFileName)

			// upload zip file to blob and set artifactUrl
			val result: Array[String] = uploadArtifactToCloud(new File(zipFileName), obj.identifier)

			val updatedMeta = obj.metadata ++ Map("artifactUrl" -> result(1))
			new ObjectData(obj.identifier, updatedMeta, obj.extData, obj.hierarchy)
		} catch {
			case ex: Exception =>
				ex.printStackTrace()
				throw new Exception(s"Error While Generating $pkgType ECAR Bundle For : " + obj.identifier, ex)
		} finally {
			FileUtils.deleteDirectory(new File(bundlePath))
		}
	}

	@throws[Exception]
	def getIndexFile(identifier: String, objType: String, bundlePath: String, objList: List[Map[String, AnyRef]]): File = {
		try {
			val file: File = new File(bundlePath + File.separator + indexFileName)
			val header: String = s"""{"id": "sunbird.${objType.toLowerCase()}.archive", "ver": "$defaultManifestVersion" ,"ts":"$getTimeStamp", "params":{"resmsgid": "$getUUID"}, "archive":{ "count": ${objList.size}, "ttl":24, "items": """
			val mJson = header + ScalaJsonUtil.serialize(objList) + "}}"
			FileUtils.writeStringToFile(file, mJson)
			file
		} catch {
			case e: Exception => throw new Exception("Exception occurred while writing manifest file for : " + identifier, e)
		}
	}

	private def uploadArtifactToCloud(uploadFile: File, identifier: String)(implicit cloudStorageUtil: CloudStorageUtil): Array[String] = {
		var urlArray = new Array[String](2)
		// Check the cloud folder convention to store artifact.zip file with Mahesh
		try {
			val folder = "question" + File.separator + Slug.makeSlug(identifier, isTransliterate = true)
			urlArray = cloudStorageUtil.uploadFile(folder, uploadFile)
		} catch {
			case e: Exception =>
				cloudStorageUtil.deleteFile(uploadFile.getAbsolutePath, Option(false))
				logger.error("Error while uploading the Artifact file.", e)
				throw new Exception("Error while uploading the Artifact File.", e)
		}
		urlArray
	}
}
