package org.sunbird.job.user.pii.updater.helpers

import akka.dispatch.ExecutionContexts
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.lang3.StringUtils
import org.mockito.{ArgumentMatchers, Mockito}
import org.neo4j.driver.v1.StatementResult
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.user.pii.updater.task.UserPiiUpdaterConfig
import org.sunbird.job.util.Neo4JUtil

class Neo4jDataProcessorSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

  implicit val mockNeo4JUtil: Neo4JUtil = mock[Neo4JUtil](Mockito.withSettings().serializable())
  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  implicit val jobConfig: UserPiiUpdaterConfig = new UserPiiUpdaterConfig(config)
  implicit val ec = ExecutionContexts.global

  "updateObject" should "return identifier for successful update" in {
    val mockResult = mock[StatementResult](Mockito.withSettings().serializable())
    Mockito.when(mockNeo4JUtil.executeQuery(ArgumentMatchers.anyString())).thenReturn(mockResult)
    val result = new TestNeo4jDataProcessor().updateObject("do_123", Map("creator"->"Deleted User"))
    assert(StringUtils.equalsIgnoreCase("do_123", result))
  }

  "generateSearchQuery" should "return the search query" in {
    val result = new TestNeo4jDataProcessor().generateSearchQuery("Question", "createdBy", "1.0", "abc123", List("creator", "originData.creator.name"))
    val expectedQuery = s"""MATCH(n:domain) WHERE n.IL_FUNC_OBJECT_TYPE IN ["Question","QuestionImage"] AND n.IL_SYS_NODE_TYPE="DATA_NODE" AND n.createdBy="abc123"  AND n.schemaVersion IS null RETURN n.IL_UNIQUE_ID AS identifier, n.IL_FUNC_OBJECT_TYPE AS objectType ,n.status as status,n.visibility as visibility,n.creator as creator,n.originData as originData;"""
    assert(!result._1.isEmpty)
    assert(StringUtils.equals(result._1, expectedQuery))
    assert(!result._2.isEmpty)
    assert(result._2.contains("originData"))
  }

}

class TestNeo4jDataProcessor extends Neo4jDataProcessor {}
