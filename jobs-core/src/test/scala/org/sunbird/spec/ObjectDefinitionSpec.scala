package org.sunbird.spec

import org.apache.commons.lang.StringUtils
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.domain.`object`.ObjectDefinition

class ObjectDefinitionSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

  val schema = Map[String, AnyRef]("properties" -> Map[String, AnyRef]("instructions" -> Map("type"-> "object"), "outcomeDeclaration"-> Map("type"->"object")))
  val config = Map[String, AnyRef]("oneOfProps"-> List("body","answer"), "external" -> Map("tableName" -> "questionset_hierarchy", "properties" -> Map("hierarchy"->Map("type" -> "string"), "instructions"->Map("type" -> "string"), "outcomeDeclaration"->Map("type" -> "string")), "primaryKey" -> List("identifier")))

  val qSchema = Map[String, AnyRef]("properties" -> Map[String, AnyRef]("instructions" -> Map("type" -> "object"), "outcomeDeclaration" -> Map("type" -> "object")))

  val qConfig = Map[String, AnyRef]("oneOfProps" -> List("body", "answer"), "external" -> Map("tableName" -> "question_data", "properties" -> Map("body" -> Map("type" -> "string"), "instructions" -> Map("type" -> "string"), "outcomeDeclaration" -> Map("type" -> "string")), "primaryKey" -> List("identifier")))

  "getPropsType" should "return property and its type in map format " in {
    val objDef = new ObjectDefinition("QuestionSet","1.0", schema, config)
    val output = objDef.getPropsType(List("instructions", "outcomeDeclaration"))
    assert(output.nonEmpty)
    assert(output.size==2)
    assert(StringUtils.equalsIgnoreCase(output.getOrElse("instructions", "").asInstanceOf[String],"object"))
    assert(StringUtils.equalsIgnoreCase(output.getOrElse("outcomeDeclaration", "").asInstanceOf[String],"object"))
  }

  "getOneOfProps" should "return list of props having oneOf type" in {
    val objDef = new ObjectDefinition("Question", "1.0", qSchema, qConfig)
    val output = objDef.getOneOfProps()
    assert(output.nonEmpty)
    assert(output.size == 2)
    assert(output.contains("body"))
    assert(output.contains("answer"))
  }

  "getJsonProps" should "return list of props having object or array type" in {
    val objDef = new ObjectDefinition("Question", "1.0", qSchema, qConfig)
    val output = objDef.getJsonProps()
    assert(output.nonEmpty)
    assert(output.size == 2)
    assert(output.contains("instructions"))
    assert(output.contains("outcomeDeclaration"))
  }

  "getExternalTable" should "return table name" in {
    val objDef = new ObjectDefinition("Question", "1.0", qSchema, qConfig)
    val output: String = objDef.getExternalTable()
    assert(output.nonEmpty)
    assert(StringUtils.equalsIgnoreCase("question_data", output))
  }

  "getExternalProps" should "return external property and its type in map format" in {
    val objDef = new ObjectDefinition("Question", "1.0", qSchema, qConfig)
    val output = objDef.getExternalProps()
    assert(output.nonEmpty)
    assert(output.size == 3)
    assert(output.contains("body"))
    assert(output.contains("instructions"))
    assert(output.contains("outcomeDeclaration"))
  }

  "getExternalTable" should "primary key of external table" in {
    val objDef = new ObjectDefinition("Question", "1.0", qSchema, qConfig)
    val output = objDef.getExternalPrimaryKey()
    assert(output.nonEmpty)
    assert(output.size==1)
    assert(output.contains("identifier"))
  }
}
