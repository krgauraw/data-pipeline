package org.sunbird.job.user.pii.updater.task

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.lang3.StringUtils
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar

class UserPiiUpdaterConfigSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  val jobConfig: UserPiiUpdaterConfig = new UserPiiUpdaterConfig(config)

  "it" should "return required configurations" in {
    assert(StringUtils.equalsIgnoreCase("local", jobConfig.jobEnv))
    assert(StringUtils.equalsIgnoreCase("local.delete.user.job.request", jobConfig.kafkaInputTopic))
    assert(StringUtils.equalsIgnoreCase("user-pii-data-updater-consumer", jobConfig.inputConsumerName))
    assert(1 == jobConfig.kafkaConsumerParallelism)
    assert(1 == jobConfig.eventRouterParallelism)
    assert(1 == jobConfig.userPiiDataUpdaterParallelism)
    assert(StringUtils.equalsIgnoreCase("total-events-count", jobConfig.totalEventsCount))
    assert(StringUtils.equalsIgnoreCase("skipped-event-count", jobConfig.skippedEventCount))
    assert(StringUtils.equalsIgnoreCase("user-pii-update-count", jobConfig.userPiiUpdateCount))
    assert(StringUtils.equalsIgnoreCase("user-pii-update-success-count", jobConfig.userPiiUpdateSuccessEventCount))
    assert(StringUtils.equalsIgnoreCase("user-pii-update-failed-count", jobConfig.userPiiUpdateFailedEventCount))
    assert(StringUtils.equalsIgnoreCase("user-pii-update-skipped-count", jobConfig.userPiiUpdateSkippedEventCount))
    assert(StringUtils.equalsIgnoreCase("user-pii-update-partial-success-count", jobConfig.userPiiUpdatePartialSuccessEventCount))
    assert(StringUtils.equalsIgnoreCase("bolt://localhost:7687", jobConfig.graphRoutePath))
    assert(StringUtils.equalsIgnoreCase("domain", jobConfig.graphName))
    assert(StringUtils.equalsIgnoreCase("Deleted User", jobConfig.user_pii_replacement_value))
    assert(StringUtils.equalsIgnoreCase("https://sunbirddev.blob.core.windows.net/sunbird-content-dev/schemas/local", jobConfig.definitionBasePath))
    assert(StringUtils.equalsIgnoreCase("http://localhost:9000/userorg", jobConfig.userorg_service_baseUrl))
    assert(StringUtils.equalsIgnoreCase("User Account Deletion Notification", jobConfig.notification_email_subject))
    assert(StringUtils.equalsIgnoreCase("Team", jobConfig.notification_email_regards))
    assert(jobConfig.admin_email_notification_enable)
  }

}
