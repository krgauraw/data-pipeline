package org.sunbird.job.quml.migrator.helpers

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.lang.StringUtils
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.job.domain.`object`.ObjectDefinition
import org.sunbird.job.quml.migrator.domain.ObjectData
import org.sunbird.job.util.Neo4JUtil

import java.util

class QumlMigratorSpec extends FlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {

  val mapper = new ObjectMapper()

  "processMaxScore" should "return the data after removing maxScore" in {
    val obj = new TestQumlMigrator()
    val data = new util.HashMap[String, AnyRef]() {
      {
        put("maxScore", 1.asInstanceOf[AnyRef])
      }
    }
    obj.processMaxScore(data)
    assert(!data.containsKey("maxScore"))
    assert(data.containsKey("outcomeDeclaration"))
  }

  "processInstructions" should "return the data having instructions in new format" in {
    val obj = new TestQumlMigrator()
    val data = new util.HashMap[String, AnyRef]() {
      {
        put("instructions", new util.HashMap[String, AnyRef]() {
          {
            put("default", "sample instructions")
          }
        })
      }
    }
    obj.processInstructions(data)
    assert(data.containsKey("instructions"))
    assert(StringUtils.equalsIgnoreCase("sample instructions", data.getOrDefault("instructions", "").asInstanceOf[String]))
  }

  "processBloomsLevel" should "return the data having complexityLevel" in {
    val obj = new TestQumlMigrator()
    val data = new util.HashMap[String, AnyRef]() {
      {
        put("bloomsLevel", "create")
      }
    }
    obj.processBloomsLevel(data)
    assert(!data.containsKey("bloomsLevel"))
    assert(data.containsKey("complexityLevel"))
  }

  "processBooleanProps" should "return the data having boolean types" in {
    val obj = new TestQumlMigrator()
    val data = new util.HashMap[String, AnyRef]() {
      {
        put("showSolutions", "Yes")
        put("showFeedback", "Yes")
        put("showHints", "Yes")
        put("showTimer", "No")
      }
    }
    obj.processBooleanProps(data)
    assert(data.getOrDefault("showSolutions", false.asInstanceOf[AnyRef]).asInstanceOf[Boolean])
    assert(data.getOrDefault("showFeedback", false.asInstanceOf[AnyRef]).asInstanceOf[Boolean])
    assert(data.getOrDefault("showHints", false.asInstanceOf[AnyRef]).asInstanceOf[Boolean])
    assert(!data.getOrDefault("showTimer", true.asInstanceOf[AnyRef]).asInstanceOf[Boolean])
  }

  "processTimeLimits" should "return the data having updated format for timeLimits" in {
    val obj = new TestQumlMigrator()
    val data = new util.HashMap[String, AnyRef]() {
      {
        put("timeLimits", new util.HashMap[String, AnyRef]() {
          {
            put("maxTime", "60")
            put("warningTime", "20")
          }
        })
      }
    }
    obj.processTimeLimits(data)
    val timeLimits = data.getOrDefault("timeLimits", new util.HashMap()).asInstanceOf[util.Map[String, AnyRef]]
    val qs = timeLimits.getOrDefault("questionSet", new util.HashMap()).asInstanceOf[util.Map[String, AnyRef]]
    assert(!qs.isEmpty)
    assert(qs.containsKey("max"))
    assert(qs.containsKey("min"))
    assert(60 == qs.getOrDefault("max", 0.asInstanceOf[AnyRef]).asInstanceOf[Integer])
    assert(0 == qs.getOrDefault("min", 1.asInstanceOf[AnyRef]).asInstanceOf[Integer])
  }

  "getAnswer" should "return answer field for multiple choice question" in {
    val obj = new TestQumlMigrator()
    val respStr = """{"responseDeclaration":{"response1":{"cardinality":"single","type":"integer","correctResponse":{"value":0}}}}"""
    val responseDeclaration = mapper.readValue(respStr, classOf[util.Map[String, AnyRef]])
    val data = new util.HashMap[String, AnyRef]() {
      {
        put("interactions", new util.HashMap[String, AnyRef]() {
          {
            put("response1", new util.HashMap[String, AnyRef]() {
              {
                put("type", "choice")
                put("options", new util.ArrayList[util.Map[String, AnyRef]]() {
                  {
                    add(new util.HashMap[String, AnyRef]() {
                      {
                        put("label", "<p>Delhi</p>")
                        put("value", 0.asInstanceOf[AnyRef])
                      }
                    })
                    add(new util.HashMap[String, AnyRef]() {
                      {
                        put("label", "<p>Chennai</p>")
                        put("value", 1.asInstanceOf[AnyRef])
                      }
                    })
                    add(new util.HashMap[String, AnyRef]() {
                      {
                        put("label", "<p>Bengaluru</p>")
                        put("value", 2.asInstanceOf[AnyRef])
                      }
                    })
                  }
                })
              }
            })
            put("validation", new util.HashMap[String, AnyRef]() {
              {
                put("required", true.asInstanceOf[AnyRef])
              }
            })
          }
        })
        putAll(responseDeclaration)
        put("primaryCategory", "Multiple Choice Question")
      }
    }
    val output = obj.getAnswer(data)
    assert(StringUtils.isNotBlank(output))
    assert(output.contains("<p>Delhi</p>"))
  }

  "processInteractions" should "return interactions in new format" in {
    val obj = new TestQumlMigrator()
    val data = new util.HashMap[String, AnyRef]() {
      {
        put("interactions", new util.HashMap[String, AnyRef]() {
          {
            put("response1", new util.HashMap[String, AnyRef]() {
              {
                put("type", "choice")
                put("options", new util.ArrayList[util.Map[String, AnyRef]]() {
                  {
                    add(new util.HashMap[String, AnyRef]() {
                      {
                        put("label", "<p>Delhi</p>")
                        put("value", 0.asInstanceOf[AnyRef])
                      }
                    })
                    add(new util.HashMap[String, AnyRef]() {
                      {
                        put("label", "<p>Chennai</p>")
                        put("value", 1.asInstanceOf[AnyRef])
                      }
                    })
                    add(new util.HashMap[String, AnyRef]() {
                      {
                        put("label", "<p>Bengaluru</p>")
                        put("value", 2.asInstanceOf[AnyRef])
                      }
                    })
                  }
                })
              }
            })
            put("validation", new util.HashMap[String, AnyRef]() {
              {
                put("required", true.asInstanceOf[AnyRef])
              }
            })
          }
        })
      }
    }
    obj.processInteractions(data)
    val resp1 = data.getOrDefault("interactions", new util.HashMap()).asInstanceOf[util.Map[String, AnyRef]].getOrDefault("response1", new util.HashMap()).asInstanceOf[util.Map[String, AnyRef]]
    assert(resp1.containsKey("validation"))
  }

  "processHints" should "return updated hints" in {
    val data = new util.HashMap[String, AnyRef]() {
      {
        put("hints", new util.ArrayList[String]() {
          {
            add("Hint-1")
            add("Hint-2")
          }
        })
      }
    }
    val obj = new TestQumlMigrator()
    obj.processHints(data)
    val hints = data.getOrDefault("hints", new util.HashMap()).asInstanceOf[util.Map[String, AnyRef]]
    assert(!hints.isEmpty)
    assert(hints.size() == 2)
  }

  "processResponseDeclaration" should "return updated responseDeclaration" in {
    val respStr = """{"responseDeclaration":{"response1":{"maxScore":2,"cardinality":"single","type":"integer","correctResponse":{"value":"1","outcomes":{"SCORE":2}},"mapping":[{"response":1,"outcomes":{"score":2}}]}}}"""
    val data = mapper.readValue(respStr, classOf[util.Map[String, AnyRef]])
    val obj = new TestQumlMigrator()
    obj.processResponseDeclaration(data)
    println("data ::: " + data)
    val correctResponse = data.get("responseDeclaration").asInstanceOf[util.Map[String, AnyRef]].get("response1").asInstanceOf[util.Map[String, AnyRef]].get("correctResponse").asInstanceOf[util.Map[String, AnyRef]]
    assert(1 == correctResponse.get("value").asInstanceOf[Integer])
    assert(data.containsKey("outcomeDeclaration"))
  }

  "processSolutions" should "return solution data" in {
    val obj = new TestQumlMigrator()
    val data = new util.HashMap[String, AnyRef]()
    val solStr = """{"solutions":[{"id":"c5edd37b-657d-4d18-82bb-7c7f8e1626be","type":"video","value":"do_2137992180697333761413"}]}"""
    val mediaStr = """{"media":[{"id":"do_2137992180697333761413","src":"/assets/public/content/assets/do_2137992180697333761413/earth.mp4","type":"video","assetId":"do_2137992180697333761413","name":"earth","baseUrl":"https://dev.inquiry.sunbird.org"}]}"""
    val solutions = mapper.readValue(solStr, classOf[util.Map[String, AnyRef]])
    val media = mapper.readValue(mediaStr, classOf[util.Map[String, AnyRef]])
    data.putAll(solutions)
    data.putAll(media)
    obj.processSolutions(data)
    val solOut = data.get("solutions").asInstanceOf[util.Map[String, AnyRef]]
    assert(solOut.size() == 1)
  }

  "getRepublishEvent" should "return kafka event for republish" in {
    val data = Map[String, AnyRef]("identifier" -> "do_123", "pkgVersion" -> 1.asInstanceOf[AnyRef], "objectType" -> "Question", "mimeType" -> "application/vnd.sunbird.question", "status" -> "Live", "schemaVersion" -> "1.1", "channel" -> "test", "lastPublishedBy" -> "test-user")
    val obj = new TestQumlMigrator()
    val event = obj.getRepublishEvent(data, "question-republish", "local")
    assert(StringUtils.isNotBlank(event))
    assert(event.contains("republish"))
  }

}

class TestQumlMigrator extends QumlMigrator {

  override def migrateQuestion(data: ObjectData)(implicit definition: ObjectDefinition): Option[ObjectData] = None

  override def migrateQuestionSet(data: ObjectData)(implicit definition: ObjectDefinition, neo4JUtil: Neo4JUtil): Option[ObjectData] = None

}
