package org.sunbird.job.util

import org.sunbird.cloud.storage.BaseStorageService
import org.sunbird.cloud.storage.factory.{StorageConfig, StorageServiceFactory}
import org.sunbird.job.BaseJobConfig

import java.io.File

object StaticCloudStorageUtil extends Serializable {
  var config: BaseJobConfig = null

  def setConfig(baseConfig: BaseJobConfig): Unit = {
    this.config = baseConfig
  }

  var storageService: BaseStorageService = null
  val container: String = getContainerName
  
  @throws[Exception]
  def getService: BaseStorageService = {

    if (null == storageService) {
      val cloudStorageType: String = StaticCloudStorageUtil.config.getString("cloud_storage_type", "azure")
      val storageKey = StaticCloudStorageUtil.config.getString("cloud_storage_key", "")
      val storageSecret = StaticCloudStorageUtil.config.getString("cloud_storage_secret", "").replace("\\n", "\n")
      val endPoint = Option(StaticCloudStorageUtil.config.getString("cloud_storage_endpoint", ""))
      // TODO: endPoint defined to support "cephs3". Make code changes after cloud-store-sdk 2.11 support it.
      println("StorageService --> params: " +  cloudStorageType + "," + storageKey)
      storageService = StorageServiceFactory.getStorageService(new StorageConfig(cloudStorageType, storageKey, storageSecret, endPoint))
    }
    storageService
  }

  def getContainerName: String = {
    StaticCloudStorageUtil.config.getString("cloud_storage_container", "")
  }

  def uploadFile(folderName: String, file: File, slug: Option[Boolean] = Option(true), container: String = container): Array[String] = {
    val slugFile = if (slug.getOrElse(true)) Slug.createSlugFile(file) else file
    val objectKey = folderName + "/" + slugFile.getName
    val url = getService.upload(container, slugFile.getAbsolutePath, objectKey, Option.apply(false), Option.apply(1), Option.apply(5), Option.empty)
    Array[String](objectKey, url)
  }

  def copyObjectsByPrefix(sourcePrefix: String, destinationPrefix: String, isFolder: Boolean): Unit = {
    getService.copyObjects(container, sourcePrefix, container, destinationPrefix, Option.apply(isFolder))
  }

  def getURI(prefix: String, isDirectory: Option[Boolean]): String = {
    getService.getUri(getContainerName, prefix, isDirectory)
  }
  
  def uploadDirectory(folderName: String, directory: File, slug: Option[Boolean] = Option(true)): Array[String] = {
    val slugFile = if (slug.getOrElse(true)) Slug.createSlugFile(directory) else directory
    val objectKey = folderName + File.separator
    val url = getService.upload(getContainerName, slugFile.getAbsolutePath, objectKey, Option.apply(true), Option.apply(1), Option.apply(5), Option.empty)
    Array[String](objectKey, url)
  }

  def deleteFile(key: String, isDirectory: Option[Boolean] = Option(false)): Unit = {
    getService.deleteObject(getContainerName, key, isDirectory)
  }

  def downloadFile(downloadPath: String, file: String, slug: Option[Boolean] = Option(false)): Unit = {
    getService.download(getContainerName, file, downloadPath, slug)
  }

  def getSignedUrl(container: String, path: String, ttl: Int): String = {
    return getService.getPutSignedURL(container, path, Option.apply(ttl), Option.apply("r"), Option.empty)
  }

}
