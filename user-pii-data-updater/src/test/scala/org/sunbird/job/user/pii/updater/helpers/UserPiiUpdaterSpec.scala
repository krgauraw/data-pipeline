package org.sunbird.job.user.pii.updater.helpers

import akka.dispatch.ExecutionContexts
import com.typesafe.config.{Config, ConfigFactory}
import org.mockito.Mockito
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.user.pii.updater.domain.ObjectData
import org.sunbird.job.user.pii.updater.task.UserPiiUpdaterConfig
import org.sunbird.job.util.{HttpUtil, Neo4JUtil}

class UserPiiUpdaterSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

  implicit val mockNeo4JUtil: Neo4JUtil = mock[Neo4JUtil](Mockito.withSettings().serializable())
  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  implicit val jobConfig: UserPiiUpdaterConfig = new UserPiiUpdaterConfig(config)
  implicit val ec = ExecutionContexts.global
  implicit val httpUtil = new HttpUtil

  override protected def beforeAll(): Unit = {
    super.beforeAll()
   }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }

  "processNestedProp" should "return a map with updated value" in {
    val data = new ObjectData("do_123", Map("objectType"->"Question","status"->"Draft","originData"->"{\\\"creator\\\":{\\\"name\\\":\\\"Test User\\\"}}"))
    val result: Map[String, AnyRef] = new TestUserPiiUpdater().processNestedProp("originData.creator.name",data)(jobConfig)
    val originData = result.getOrElse("originData", "").asInstanceOf[String]
    assert(originData.contains("Deleted User"))
  }

}

class TestUserPiiUpdater extends UserPiiUpdater {}
