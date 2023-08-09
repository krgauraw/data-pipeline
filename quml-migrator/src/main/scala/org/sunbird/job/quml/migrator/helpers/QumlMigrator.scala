package org.sunbird.job.quml.migrator.helpers

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.lang.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.domain.`object`.ObjectDefinition
import org.sunbird.job.quml.migrator.domain.ObjectData
import org.sunbird.job.util.{JSONUtil, Neo4JUtil}

import java.util
import java.util.UUID
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

trait QumlMigrator {

  private[this] val logger = LoggerFactory.getLogger(classOf[QumlMigrator])

  protected val mapper = new ObjectMapper()

  def migrateQuestion(data: ObjectData)(implicit definition: ObjectDefinition): Option[ObjectData]

  def migrateQuestionSet(data: ObjectData)(implicit definition: ObjectDefinition, neo4JUtil: Neo4JUtil): Option[ObjectData]

  def processMaxScore(data: util.Map[String, AnyRef]): Unit = {
    if (data.containsKey("maxScore")) {
      val maxScore = data.remove("maxScore")
      val outcomeDeclaration = Map("maxScore" -> Map("cardinality" -> "single", "type" -> "integer", "defaultValue" -> maxScore).asJava).asJava
      data.put("outcomeDeclaration", outcomeDeclaration)
    }
  }

  def processInstructions(data: util.Map[String, AnyRef]): Unit = {
    if (data.containsKey("instructions")) {
      val instructions = data.getOrDefault("instructions", new util.HashMap[String, AnyRef]()).asInstanceOf[util.Map[String, AnyRef]]
      if (!instructions.isEmpty && (instructions.keySet().size() == 1 && instructions.keySet().contains("default"))) {
        data.put("instructions", instructions.get("default").asInstanceOf[String])
      }
    }
  }

  def processBloomsLevel(data: util.Map[String, AnyRef]): Unit = {
    if (data.containsKey("bloomsLevel")) {
      val bLevel = data.remove("bloomsLevel")
      data.put("complexityLevel", List(bLevel.toString).asJava)
    }
  }

  def processBooleanProps(data: util.Map[String, AnyRef]): Unit = {
    val booleanProps = List("showSolutions", "showFeedback", "showHints", "showTimer")
    booleanProps.foreach(prop => {
      if (data.containsKey(prop)) {
        val propVal: String = data.get(prop).asInstanceOf[String]
        data.put(prop, getBooleanValue(propVal).asInstanceOf[AnyRef])
      }
    })
  }

  def processTimeLimits(data: util.Map[String, AnyRef]): Unit = {
    if (data.containsKey("timeLimits")) {
      val timeLimits: util.Map[String, AnyRef] = if (data.get("timeLimits").isInstanceOf[util.Map[String, AnyRef]]) data.getOrDefault("timeLimits", Map().asJava).asInstanceOf[util.Map[String, AnyRef]] else mapper.readValue(data.get("timeLimits").asInstanceOf[String], classOf[util.Map[String, AnyRef]])
      val maxTime: Integer = timeLimits.getOrDefault("maxTime", "0").asInstanceOf[String].toInt
      val updatedData: util.Map[String, AnyRef] = Map("questionSet" -> Map("max" -> maxTime, "min" -> 0.asInstanceOf[AnyRef]).asJava).asJava.asInstanceOf[util.Map[String, AnyRef]]
      data.put("timeLimits", updatedData)
    }
  }

  def getAnswer(data: util.Map[String, AnyRef]): String = {
    val interactions: util.Map[String, AnyRef] = data.getOrDefault("interactions", Map[String, AnyRef]().asJava).asInstanceOf[util.Map[String, AnyRef]]
    if (!StringUtils.equalsIgnoreCase(data.getOrElse("primaryCategory", "").asInstanceOf[String], "Subjective Question") && !interactions.isEmpty) {
      val responseDeclaration: util.Map[String, AnyRef] = data.getOrDefault("responseDeclaration", Map[String, AnyRef]().asJava).asInstanceOf[util.Map[String, AnyRef]]
      val responseData = responseDeclaration.get("response1").asInstanceOf[util.Map[String, AnyRef]]
      val intractionsResp1: util.Map[String, AnyRef] = interactions.getOrElse("response1", new util.HashMap[String, AnyRef]()).asInstanceOf[util.Map[String, AnyRef]]
      val options = intractionsResp1.getOrElse("options", List().asJava).asInstanceOf[util.List[util.Map[String, AnyRef]]]
      val answerData = responseData.get("cardinality").asInstanceOf[String] match {
        case "single" => {
          val correctResp = responseData.getOrDefault("correctResponse", Map().asJava).asInstanceOf[util.Map[String, AnyRef]].get("value").asInstanceOf[Integer]
          val label = options.toList.filter(op => op.get("value").asInstanceOf[Integer] == correctResp).head.get("label").asInstanceOf[String]
          val answer = """<div class="anwser-container"><div class="anwser-body">answer_html</div></div>""".replace("answer_html", label)
          answer
        }
        case "multiple" => {
          val correctResp = responseData.getOrDefault("correctResponse", Map().asJava).asInstanceOf[util.Map[String, AnyRef]].get("value").asInstanceOf[util.List[Integer]]
          val singleAns = """<div class="anwser-body">answer_html</div>"""
          val answerList: List[String] = options.toList.filter(op => correctResp.contains(op.get("value").asInstanceOf[Integer])).map(op => singleAns.replace("answer_html", op.get("label").asInstanceOf[String])).toList
          val answer = """<div class="anwser-container">answer_div</div>""".replace("answer_div", answerList.mkString(""))
          answer
        }
      }
      answerData
    } else data.getOrDefault("answer", "").asInstanceOf[String]
  }

  def processInteractions(data: util.Map[String, AnyRef]): Unit = {
    val interactions: util.Map[String, AnyRef] = data.getOrDefault("interactions", Map[String, AnyRef]().asJava).asInstanceOf[util.Map[String, AnyRef]]
    if (!interactions.isEmpty) {
      val validation = interactions.getOrElse("validation", new util.HashMap[String, AnyRef]()).asInstanceOf[util.Map[String, AnyRef]]
      val resp1: util.Map[String, AnyRef] = interactions.getOrElse("response1", new util.HashMap[String, AnyRef]()).asInstanceOf[util.Map[String, AnyRef]]
      val resValData = interactions.getOrElse("response1", new util.HashMap[String, AnyRef]()).asInstanceOf[util.Map[String, AnyRef]].getOrElse("validation", new util.HashMap[String, AnyRef]()).asInstanceOf[util.Map[String, AnyRef]]
      if (!resValData.isEmpty) resValData.putAll(validation) else resp1.put("validation", validation)
      interactions.remove("validation")
      interactions.put("response1", resp1)
      data.put("interactions", interactions)
    }
  }

  def processHints(data: util.Map[String, AnyRef]): Unit = {
    val hints = data.getOrDefault("hints", List[String]().asJava).asInstanceOf[util.List[String]]
    if (!hints.isEmpty) {
      val updatedHints: util.Map[String, AnyRef] = hints.toList.map(hint => Map[String, AnyRef](UUID.randomUUID().toString -> hint).asJava).flatten.toMap.asJava
      data.put("hints", updatedHints)
    }
  }

  def processResponseDeclaration(data: util.Map[String, AnyRef]): Unit = {
    val outcomeDeclaration = new util.HashMap[String, AnyRef]()
    // Remove responseDeclaration metadata for Subjective Question
    if (StringUtils.equalsIgnoreCase("Subjective Question", data.getOrDefault("primaryCategory", "").toString)) {
      data.remove("responseDeclaration")
      data.remove("interactions")
      if (data.containsKey("maxScore") && null != data.get("maxScore")) {
        data.put("outcomeDeclaration", Map[String, AnyRef]("cardinality" -> "single", "type" -> "integer", "defaultValue" -> data.get("maxScore")).asJava)
      }
    } else {
      //transform responseDeclaration and populate outcomeDeclaration
      val responseDeclaration: util.Map[String, AnyRef] = data.getOrDefault("responseDeclaration", Map[String, AnyRef]().asJava).asInstanceOf[util.Map[String, AnyRef]]
      if (!responseDeclaration.isEmpty) {
        for (key <- responseDeclaration.keySet()) {
          val responseData = responseDeclaration.get(key).asInstanceOf[util.Map[String, AnyRef]]
          // remove maxScore and put it under outcomeDeclaration
          val maxScore = Map[String, AnyRef]("cardinality" -> responseData.getOrDefault("cardinality", "").asInstanceOf[String], "type" -> responseData.getOrDefault("type", "").asInstanceOf[String], "defaultValue" -> responseData.get("maxScore"))
          responseData.remove("maxScore")
          outcomeDeclaration.put("maxScore", maxScore.asJava)
          //remove outcome from correctResponse
          responseData.getOrDefault("correctResponse", Map[String, AnyRef]().asJava).asInstanceOf[util.Map[String, AnyRef]].remove("outcomes")
          // type cast value. data type mismatch seen in quml 1.0 data where type and data was integer but integer data was populated as string
          try {
            if (StringUtils.equalsIgnoreCase("integer", responseData.getOrDefault("type", "").asInstanceOf[String])
              && StringUtils.equalsIgnoreCase("single", responseData.getOrDefault("cardinality", "").asInstanceOf[String])) {
              val correctResp: util.Map[String, AnyRef] = responseData.getOrDefault("correctResponse", Map[String, AnyRef]().asJava).asInstanceOf[util.Map[String, AnyRef]]
              val correctKey: String = correctResp.getOrDefault("value", "0").asInstanceOf[String]
              correctResp.put("value", correctKey.toInt.asInstanceOf[AnyRef])
            }
          } catch {
            case e: NumberFormatException => e.printStackTrace()
          }
          //update mapping
          val mappingData = responseData.getOrElse("mapping", List[util.Map[String, AnyRef]]().asJava).asInstanceOf[util.List[util.Map[String, AnyRef]]]
          if (!mappingData.isEmpty) {
            val updatedMapping = mappingData.asScala.toList.map(mapData => {
              Map[String, AnyRef]("value" -> mapData.get("response"), "score" -> mapData.getOrDefault("outcomes", Map[String, AnyRef]().asJava).asInstanceOf[util.Map[String, AnyRef]].get("score")).asJava
            }).asJava
            responseData.put("mapping", updatedMapping)
          }
        }
        data.put("responseDeclaration", responseDeclaration)
        data.put("outcomeDeclaration", outcomeDeclaration)
      }
    }
  }

  def processSolutions(data: util.Map[String, AnyRef]): Unit = {
    val solutions = data.getOrDefault("solutions", List[util.Map[String, AnyRef]](Map[String, AnyRef]().asJava).asJava).asInstanceOf[util.List[util.Map[String, AnyRef]]]
    if (!solutions.isEmpty) {
      val updatedSolutions: util.Map[String, AnyRef] = solutions.asScala.toList.map(solution => {
        Map[String, AnyRef](solution.getOrElse("id", "").asInstanceOf[String] -> getSolutionString(solution, data.getOrElse("media", List[util.Map[String, AnyRef]](Map[String, AnyRef]().asJava).asJava).asInstanceOf[util.List[util.Map[String, AnyRef]]]))
      }).flatten.toMap.asJava
      data.put("solutions", updatedSolutions)
    }
  }

  def getBooleanValue(str: String): Boolean = {
    str match {
      case "Yes" => true
      case _ => false
    }
  }

  def getSolutionString(data: util.Map[String, AnyRef], media: util.List[util.Map[String, AnyRef]]): String = {
    if (!data.isEmpty) {
      data.getOrDefault("type", "").asInstanceOf[String] match {
        case "html" => data.getOrDefault("value", "").asInstanceOf[String]
        case "video" => {
          val value = data.getOrDefault("value", "").asInstanceOf[String]
          val mediaData: util.Map[String, AnyRef] = media.asScala.toList.filter(map => StringUtils.equalsIgnoreCase(value, map.getOrElse("id", "").asInstanceOf[String])).flatten.toMap.asJava
          val src = mediaData.getOrDefault("src", "").asInstanceOf[String]
          val thumbnail = mediaData.getOrDefault("thumbnail", "").asInstanceOf[String]
          val solutionStr = """<video data-asset-variable="media_identifier" width="400" controls="" poster="thumbnail_url"><source type="video/mp4" src="media_source_url"><source type="video/webm" src="media_source_url"></video>""".replace("media_identifier", value).replace("thumbnail_url", thumbnail).replace("media_source_url", src)
          solutionStr
        }
      }
    } else ""
  }

  def getRepublishEvent(objMetadata: Map[String, Object], actorId: String, jobEnv: String): String = {
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
    val event = s"""{"eid":"BE_JOB_REQUEST","ets":$epochTime,"mid":"LP.$epochTime.${UUID.randomUUID()}","actor":{"id":"${actorId}","type":"System"},"context":{"pdata":{"ver":"1.0","id":"org.sunbird.platform"},"channel":"${channel}","env":"${jobEnv}"},"object":{"ver":"$pkgVersion","id":"$identifier"},"edata":{"publish_type":"$publishType","metadata":{"identifier":"$identifier", "mimeType":"$mimeType","objectType":"$objectType","lastPublishedBy":"${lastPublishedBy}","pkgVersion":$pkgVersion,"schemaVersion":"$schemaVersion"},"action":"republish","iteration":1}}"""
    logger.info(s"Live ${objectType} re-publish triggered for " + identifier + " | event: " + event)
    event
  }

}
