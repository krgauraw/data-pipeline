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
    val fileUrl: String = "https://sunbirddevbbpublic.blob.core.windows.net/sunbird-content-staging/content/assets/do_2137327580080128001217/gateway-of-india.jpg"
    val downloadedFile: File = FileUtils.downloadFile(fileUrl, "/tmp/contentBundle")
    assert(downloadedFile.exists())
  }
}
