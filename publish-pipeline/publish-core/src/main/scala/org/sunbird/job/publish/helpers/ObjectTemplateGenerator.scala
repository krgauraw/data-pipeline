package org.sunbird.job.publish.helpers

import org.apache.velocity.VelocityContext
import org.apache.velocity.app.Velocity
import org.slf4j.LoggerFactory

import java.io.StringWriter
import java.util.Properties

trait ObjectTemplateGenerator {

    private[this] val logger = LoggerFactory.getLogger(classOf[ObjectTemplateGenerator])
    def handleHtmlTemplate(templateName: String, context: Map[String,AnyRef]): String = {
        logger.info("handleHtmlTemplate :::: start .... context ::: "+context)
        initVelocityEngine(templateName)
        logger.info("initVelocityEngine ... done")
        val veContext: VelocityContext = new VelocityContext()
        context.foreach(entry => veContext.put(entry._1, entry._2))
        val writer:StringWriter = new StringWriter()
        Velocity.mergeTemplate(templateName, "UTF-8", veContext, writer)
        logger.info("merge done")
        val output = writer.toString
        logger.info("handleHtmlTemplate :::: output ::: "+output)
        output
    }

    private def initVelocityEngine(templateName: String): Unit = {
        val properties = new Properties()
        if (!templateName.startsWith("http") && !templateName.startsWith("/")) {
            properties.setProperty("resource.loader", "class")
            properties.setProperty("class.resource.loader.class", "org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader")
        }
        Velocity.init(properties)
    }
}
