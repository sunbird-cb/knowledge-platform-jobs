package org.sunbird.incredible.processor.store

import java.io.File

import org.apache.commons.lang3.StringUtils
import org.sunbird.cloud.storage.BaseStorageService
import org.sunbird.cloud.storage.factory.StorageConfig
import org.sunbird.cloud.storage.factory.StorageServiceFactory
import org.sunbird.incredible.pojos.exceptions.ServerException
import org.sunbird.incredible.{JsonKeys, StorageParams, UrlManager}


class StorageService(storageParams: StorageParams) extends Serializable {

  var storageService: BaseStorageService = _
  val storageType: String = storageParams.cloudStorageType

  @throws[Exception]
  def getService: BaseStorageService = {
    if (null == storageService) {
      val storageKey = storageParams.storageKey
      val storageSecret = storageParams.storageSecret
      storageService = StorageServiceFactory.getStorageService(StorageConfig(storageType, storageKey, storageSecret, storageParams.storageEndPoint))
    }
    storageService
  }

  def getContainerName: String = {
    storageParams.containerName
  }

  def uploadFile(path: String, file: File): String = {
    val objectKey = path + file.getName
    val containerName = getContainerName
    val url = getService.upload(containerName, file.getAbsolutePath, objectKey, Option.apply(false), Option.apply(1), Option.apply(5), Option.empty)
    UrlManager.getSharableUrl(url, containerName)
  }

  def getSignedUrl(container: String, path: String, ttl: Int): String = {
    return getService.getPutSignedURL(container, path, Option.apply(ttl), Option.apply("r"), Option.empty)
  }

}
