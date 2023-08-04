package org.sunbird.job.quml.migrator.task

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.lang.StringUtils
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar

class QumlMigratorConfigSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  val jobConfig: QumlMigratorConfig = new QumlMigratorConfig(config)

  "it" should "return required configurations" in {
    assert(StringUtils.equalsIgnoreCase("local", jobConfig.jobEnv))
    assert(StringUtils.equalsIgnoreCase("local.quml.migration.job.request", jobConfig.kafkaInputTopic))
    assert(StringUtils.equalsIgnoreCase("local.assessment.republish.request", jobConfig.republishTopic))
    assert(StringUtils.equalsIgnoreCase("quml-migrator-consumer", jobConfig.inputConsumerName))
    assert(1 == jobConfig.kafkaConsumerParallelism)
    assert(1 == jobConfig.eventRouterParallelism)
    assert(StringUtils.equalsIgnoreCase("total-events-count", jobConfig.totalEventsCount))
    assert(StringUtils.equalsIgnoreCase("skipped-event-count", jobConfig.skippedEventCount))
    assert(StringUtils.equalsIgnoreCase("question-migration-count", jobConfig.questionMigrationCount))
    assert(StringUtils.equalsIgnoreCase("question-migration-success-count", jobConfig.questionMigrationSuccessEventCount))
    assert(StringUtils.equalsIgnoreCase("question-migration-failed-count", jobConfig.questionMigrationFailedEventCount))
    assert(StringUtils.equalsIgnoreCase("question-migration-skipped-count", jobConfig.questionMigrationSkippedEventCount))
    assert(StringUtils.equalsIgnoreCase("questionset-migration-count", jobConfig.questionSetMigrationEventCount))
    assert(StringUtils.equalsIgnoreCase("questionset-migration-success-count", jobConfig.questionSetMigrationSuccessEventCount))
    assert(StringUtils.equalsIgnoreCase("questionset-migration-failed-count", jobConfig.questionSetMigrationFailedEventCount))
    assert(StringUtils.equalsIgnoreCase("questionset-migration-skipped-count", jobConfig.questionSetMigrationSkippedEventCount))
    assert(StringUtils.equalsIgnoreCase("question-republish-count", jobConfig.questionRepublishEventCount))
    assert(StringUtils.equalsIgnoreCase("questionset-republish-count", jobConfig.questionSetRepublishEventCount))
    assert(StringUtils.equalsIgnoreCase("localhost", jobConfig.cassandraHost))
    assert(9142 == jobConfig.cassandraPort)
    assert(StringUtils.equalsIgnoreCase("local_hierarchy_store", jobConfig.questionSetKeyspaceName))
    assert(StringUtils.equalsIgnoreCase("questionset_hierarchy", jobConfig.questionSetTableName))
    assert(StringUtils.equalsIgnoreCase("local_question_store", jobConfig.questionKeyspaceName))
    assert(StringUtils.equalsIgnoreCase("question_data", jobConfig.questionTableName))
    assert(StringUtils.equalsIgnoreCase("bolt://localhost:7687", jobConfig.graphRoutePath))
    assert(StringUtils.equalsIgnoreCase("domain", jobConfig.graphName))
    assert(StringUtils.equalsIgnoreCase("https://sunbirddev.blob.core.windows.net/sunbird-content-dev/schemas/local", jobConfig.definitionBasePath))
  }
}
