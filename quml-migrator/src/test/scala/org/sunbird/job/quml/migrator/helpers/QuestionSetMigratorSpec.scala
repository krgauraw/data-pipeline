package org.sunbird.job.quml.migrator.helpers

import akka.dispatch.ExecutionContexts
import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.config.{Config, ConfigFactory}
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.mockito.Mockito
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.domain.`object`.{DefinitionCache, ObjectDefinition}
import org.sunbird.job.quml.migrator.domain.{ExtDataConfig, ObjectData, ObjectExtData}
import org.sunbird.job.quml.migrator.exceptions.QumlMigrationException
import org.sunbird.job.quml.migrator.task.QumlMigratorConfig
import org.sunbird.job.util.{CassandraUtil, Neo4JUtil}

class QuestionSetMigratorSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

  implicit val mockNeo4JUtil: Neo4JUtil = mock[Neo4JUtil](Mockito.withSettings().serializable())
  implicit var cassandraUtil: CassandraUtil = _
  implicit val defCache = new DefinitionCache()
  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  implicit val jobConfig: QumlMigratorConfig = new QumlMigratorConfig(config)
  implicit val definition: ObjectDefinition = defCache.getDefinition("QuestionSet", "1.0", jobConfig.definitionBasePath)
  implicit val readerConfig = ExtDataConfig(jobConfig.questionSetKeyspaceName, definition.getExternalTable, definition.getExternalPrimaryKey, definition.getExternalProps)
  implicit val ec = ExecutionContexts.global
  val mapper = new ObjectMapper()

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

  "validateQuestionSet" should "validate and return error message" in {
    val data = new ObjectData("do_123", Map[String, AnyRef]("name" -> "QuestionSet-1", "identifier" -> "do_21385612453673369613688", "mimeType"->"application/vnd.sunbird.questionset"), Some(Map[String, AnyRef]("instructions" -> "{\"default\":\"sample instructions\"}")), Some(Map()))
    val result: List[String] = new TestQuestionMigrator().validateQuestion("do_21385612453673369613688", data)
    result.size should be(3)
  }

  "migrateHierarchy" should "throw an error if any children is not migrated" in {
    val hStr = """{"identifier":"do_21385612453673369613688","children":[{"parent":"do_21385612453673369613688","instructions":{"default":"sample instructions"},"code":"section-1","allowSkip":"Yes","containsUserData":"No","description":"Section-1","language":["English"],"mimeType":"application/vnd.sunbird.questionset","showHints":"No","createdOn":"2023-08-07T15:17:48.218+0000","objectType":"QuestionSet","scoreCutoffType":"AssessmentLevel","primaryCategory":"Practice Question Set","children":[{"parent":"do_21385612466764185613693","code":"q1","description":"Q1","language":["English"],"mimeType":"application/vnd.sunbird.question","createdOn":"2023-08-07T15:17:48.208+0000","objectType":"Question","primaryCategory":"Multiple Choice Question","contentDisposition":"inline","lastUpdatedOn":"2023-08-07T15:17:48.208+0000","contentEncoding":"gzip","showSolutions":"No","allowAnonymousAccess":"Yes","identifier":"do_21385612466755993613689","lastStatusChangedOn":"2023-08-07T15:17:48.208+0000","visibility":"Parent","showTimer":"No","index":1,"qType":"MCQ","maxScore":1,"languageCode":["en"],"bloomsLevel":"create","version":1,"versionKey":"1691421468218","showFeedback":"No","license":"CC BY 4.0","interactionTypes":["choice"],"depth":2,"compatibilityLevel":4,"name":"Q1","status":"Draft"},{"parent":"do_21385612466764185613693","code":"q2","description":"Q2","language":["English"],"mimeType":"application/vnd.sunbird.question","createdOn":"2023-08-07T15:17:48.217+0000","objectType":"Question","primaryCategory":"Subjective Question","contentDisposition":"inline","lastUpdatedOn":"2023-08-07T15:17:48.217+0000","contentEncoding":"gzip","showSolutions":"No","allowAnonymousAccess":"Yes","identifier":"do_21385612466763366413691","lastStatusChangedOn":"2023-08-07T15:17:48.217+0000","visibility":"Parent","showTimer":"Yes","index":2,"qType":"SA","maxScore":1,"languageCode":["en"],"version":1,"versionKey":"1691421468219","showFeedback":"No","license":"CC BY 4.0","depth":2,"compatibilityLevel":4,"name":"Q2","status":"Draft"}],"contentDisposition":"inline","lastUpdatedOn":"2023-08-07T15:17:48.218+0000","contentEncoding":"gzip","generateDIALCodes":"No","showSolutions":"No","trackable":{"enabled":"No","autoBatch":"No"},"allowAnonymousAccess":"Yes","identifier":"do_21385612466764185613693","lastStatusChangedOn":"2023-08-07T15:17:48.218+0000","requiresSubmit":"No","visibility":"Parent","showTimer":"Yes","index":1,"setType":"materialised","languageCode":["en"],"version":1,"versionKey":"1691421468218","showFeedback":"No","license":"CC BY 4.0","depth":1,"compatibilityLevel":5,"name":"Section-1","navigationMode":"non-linear","allowBranching":"No","shuffle":true,"status":"Draft"}]}"""
    val hierarchy = mapper.readValue(hStr, classOf[java.util.Map[String, AnyRef]])
    val obj = new TestQuestionSetMigrator()
    intercept[QumlMigrationException] {
      val migrHierarchy = obj.migrateHierarchy("do_21385612453673369613688",hierarchy)
    }
  }

  "migrateGrpahData" should "return migrated metadata" in {
    val metaStr = """{"code":"sunbird.qs.1","allowSkip":"Yes","containsUserData":"No","language":["English"],"mimeType":"application/vnd.sunbird.questionset","showHints":"No","createdOn":"2023-08-07T15:17:32.246+0000","objectType":"QuestionSet","scoreCutoffType":"AssessmentLevel","primaryCategory":"Practice Question Set","contentDisposition":"inline","lastUpdatedOn":"2023-08-07T15:17:48.245+0000","contentEncoding":"gzip","generateDIALCodes":"No","showSolutions":"Yes","trackable":{"enabled":"No","autoBatch":"No"},"allowAnonymousAccess":"Yes","identifier":"do_21385612453673369613688","lastStatusChangedOn":"2023-08-07T15:17:32.246+0000","requiresSubmit":"No","visibility":"Default","showTimer":"Yes","childNodes":["do_21385612466755993613689","do_21385612466764185613693","do_21385612466763366413691"],"maxScore":2,"setType":"materialised","languageCode":["en"],"bloomsLevel":"create","version":1,"versionKey":"1691421468245","showFeedback":"No","license":"CC BY 4.0","depth":0,"createdBy":"sunbird-user-1","compatibilityLevel":5,"name":"QuestionSet-Migr-Test","navigationMode":"non-linear","allowBranching":"No","timeLimits":{"maxTime":"240","warningTime":"60"},"shuffle":true,"status":"Draft"}"""
    val data = mapper.readValue(metaStr, classOf[java.util.Map[String, AnyRef]])
    val obj = new TestQuestionSetMigrator()
    val migrMeta = obj.migrateGrpahData("do_21385612453673369613688", data)
    assert(!migrMeta.containsKey("maxScore"))
    assert(!migrMeta.containsKey("bloomsLevel"))
    assert(migrMeta.containsKey("complexityLevel"))
  }

  /*"getExtData" should "return external data for given identifier" in {
    val obj = new TestQuestionSetMigrator()
    val res: Option[ObjectExtData] = obj.getExtData("do_21385612453673369613688", readerConfig)
    val result: Option[Map[String, AnyRef]] = res.getOrElse(new ObjectExtData).hierarchy
    result.getOrElse(Map()) should not be empty
  }*/
  
}

class TestQuestionSetMigrator extends QuestionSetMigrator {

}
