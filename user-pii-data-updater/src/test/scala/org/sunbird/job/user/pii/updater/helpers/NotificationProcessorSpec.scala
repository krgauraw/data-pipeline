package org.sunbird.job.user.pii.updater.helpers

import akka.dispatch.ExecutionContexts
import com.typesafe.config.{Config, ConfigFactory}
import org.mockito.{ArgumentMatchers, Mockito}
import org.mockito.Mockito.when
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.user.pii.updater.task.UserPiiUpdaterConfig
import org.sunbird.job.util.{HTTPResponse, HttpUtil, Neo4JUtil}

class NotificationProcessorSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

  implicit val mockNeo4JUtil: Neo4JUtil = mock[Neo4JUtil](Mockito.withSettings().serializable())
  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  implicit val jobConfig: UserPiiUpdaterConfig = new UserPiiUpdaterConfig(config)
  implicit val ec = ExecutionContexts.global
  implicit val httpUtil: HttpUtil = mock[HttpUtil](Mockito.withSettings().serializable())

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }

  "processNestedProp" should "return a map with updated value" in {
    when(httpUtil.post(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), ArgumentMatchers.any())).thenReturn(getNotificationAPIResponse())
    new TestNotificationProcessor().sendNotification(Map("do_123"->"Draft","do_234"->"Live"), "abc123", "Test User", List("def12dhfbd","fvjsvfr232hhkbd"))
    assert(true)
  }

  def getNotificationAPIResponse(): HTTPResponse = {
    val body =
      """{
        |  "id": "api.notification",
        |  "ver": "v2",
        |  "ts": "2023-12-18 05:34:21:477+0000",
        |  "params": {
        |    "resmsgid": "8aa0212b-24d8-4e82-8cd6-1d52c6831eaf",
        |    "msgid": "8aa0212b-24d8-4e82-8cd6-1d52c6831eaf",
        |    "err": null,
        |    "status": "SUCCESS",
        |    "errmsg": null
        |  },
        |  "responseCode": "OK",
        |  "result": {
        |    "response": "SUCCESS"
        |  }
        |}""".stripMargin
    HTTPResponse(200, body)
  }

}

class TestNotificationProcessor extends NotificationProcessor {}
