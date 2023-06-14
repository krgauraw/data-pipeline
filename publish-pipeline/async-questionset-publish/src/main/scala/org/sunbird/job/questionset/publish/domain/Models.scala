package org.sunbird.job.questionset.publish.domain

case class PublishMetadata(identifier: String, objectType: String, mimeType: String, pkgVersion: Double, publishType: String, lastPublishedBy: String, schemaVersion: String = "1.0")
