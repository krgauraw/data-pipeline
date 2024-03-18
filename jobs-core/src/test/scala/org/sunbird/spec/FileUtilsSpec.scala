package org.sunbird.spec

import org.scalatest.{FlatSpec, Matchers}
import org.sunbird.job.util.FileUtils

import java.io.File

class FileUtilsSpec extends FlatSpec with Matchers {

  "getBasePath with empty identifier" should "return the path" in {
    val result = FileUtils.getBasePath("")
    result.nonEmpty shouldBe (true)
  }

  "downloadFile " should " download the media source file starting with http or https " in {
    val fileUrl: String = "https://sunbirddev.blob.core.windows.net/sunbird-content-dev/content/do_113252367947718656186/artifact/do_113252367947718656186_1617720706250_gitkraken.png"
    val downloadedFile: File = FileUtils.downloadFile(fileUrl, "/tmp/contentBundle")
    assert(downloadedFile.exists())
  }
}
