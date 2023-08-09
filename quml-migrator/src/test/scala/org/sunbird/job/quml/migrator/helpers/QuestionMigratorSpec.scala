package org.sunbird.job.quml.migrator.helpers

import akka.dispatch.ExecutionContexts
import com.typesafe.config.{Config, ConfigFactory}
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.mockito.Mockito
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.domain.`object`.{DefinitionCache, ObjectDefinition}
import org.sunbird.job.quml.migrator.domain.{ExtDataConfig, ObjectData, ObjectExtData}
import org.sunbird.job.quml.migrator.task.QumlMigratorConfig
import org.sunbird.job.util.{CassandraUtil, HttpUtil, Neo4JUtil}

import java.util

class QuestionMigratorSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

  implicit val mockNeo4JUtil: Neo4JUtil = mock[Neo4JUtil](Mockito.withSettings().serializable())
  implicit var cassandraUtil: CassandraUtil = _
  implicit val defCache = new DefinitionCache()
  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  implicit val jobConfig: QumlMigratorConfig = new QumlMigratorConfig(config)
  implicit val definition: ObjectDefinition = defCache.getDefinition("Question", "1.0", jobConfig.definitionBasePath)
  implicit val readerConfig = ExtDataConfig(jobConfig.questionKeyspaceName, definition.getExternalTable, definition.getExternalPrimaryKey, definition.getExternalProps)


  implicit val ec = ExecutionContexts.global

  implicit val httpUtil = new HttpUtil

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(80000L)
    cassandraUtil = new CassandraUtil(jobConfig.cassandraHost, jobConfig.cassandraPort, jobConfig)
    val session = cassandraUtil.session
    val dataLoader = new CQLDataLoader(session)
    dataLoader.load(new FileCQLDataSet(getClass.getResource("/test.cql").getPath, true, true))
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    try {
      EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
      Thread.sleep(10000)
    } catch {
      case ex: Exception => {
      }
    }
  }

  "validateQuestion" should "validate and return error message for mcq question" in {
    val data = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Question-1", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef], "interactionTypes" -> new util.ArrayList[String]() {
      add("choice")
    }), Some(Map[String, AnyRef]("body" -> "sample body")))
    val result: List[String] = new TestQuestionMigrator().validateQuestion("do_123", data)
    result.size should be(4)
  }

  "validateQuestion" should "validate and return error message for sa question" in {
    val data = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Question-1", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef]), Some(Map[String, AnyRef]("body" -> "sample body")))
    val result: List[String] = new TestQuestionMigrator().validateQuestion("do_123", data)
    result.size should be(3)
  }

  "getExtData" should "return the external data for the identifier" in {
    val identifier = "do_113188615625731";
    val res: Option[ObjectExtData] = new TestQuestionMigrator().getExtData(identifier, readerConfig)
    val result: Option[Map[String, AnyRef]] = res.getOrElse(new ObjectExtData).data
    result.getOrElse(Map()).size should be(6)
  }

  "migrateQuestion" should "transform data in quml 1.1 format" in {
    val data = new ObjectData("do_123", Map[String, AnyRef]("name" -> "Question-1", "identifier" -> "do_123", "pkgVersion" -> 0.0.asInstanceOf[AnyRef], "primaryCategory"->"Subjective Question", "maxScore"->1.asInstanceOf[AnyRef], "bloomsLevel"-> "create","showHints"->"No"), Some(Map[String, AnyRef]("body" -> "sample body", "answer"->"sample answer")))
    val result: ObjectData = new TestQuestionMigrator().migrateQuestion(data).getOrElse(new ObjectData("do_234", Map()))
    result.identifier should be("do_123")
    result.metadata.contains("complexityLevel") should be(true)
    result.metadata.contains("bloomsLevel") should be(true)
    result.metadata.get("bloomsLevel").get should be(null)
  }

}

class TestQuestionMigrator extends QuestionMigrator {}
