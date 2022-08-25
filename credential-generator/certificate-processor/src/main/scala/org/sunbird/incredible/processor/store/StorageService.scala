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
      println("storageType=="+storageType)
      println("storageParams=="+storageParams)
      println("storageKey=="+Some(storageParams.storageKey))
      println("storageSecret=="+Some(storageParams.storageSecret))
      println("storageEndPoint=="+Some(storageParams.storageEndPoint))

      val storageKey = storageParams.storageKey
      val storageSecret = storageParams.storageSecret
      if (StringUtils.equalsIgnoreCase(storageType, JsonKeys.AZURE) || StringUtils.equalsIgnoreCase(storageType, JsonKeys.AWS)) {
        storageService = StorageServiceFactory.getStorageService(StorageConfig(storageType, storageKey, storageSecret))
      } else if (StringUtils.equalsIgnoreCase(storageType, JsonKeys.CEPHS3)) {
        val storageEndPoint = storageParams.storageEndPoint.getOrElse("")
        println("storageEndPoint=="+Some(storageEndPoint))
        storageService = StorageServiceFactory.getStorageService(StorageConfig(storageType, storageKey, storageSecret,Option(storageEndPoint)))
      } else throw new ServerException("ERR_INVALID_CLOUD_STORAGE", "Error while initialising cloud storage")
    }
    storageService
  }

  def getContainerName: String = {
     storageParams.containerName
  }

  def uploadFile(path: String, file: File): String = {
    val objectKey = path + file.getName
    val containerName = getContainerName
    println("containerName=="+containerName)
    val url = getService.upload(containerName, file.getAbsolutePath, objectKey, Option.apply(false), Option.apply(1), Option.apply(5), Option.empty)
    UrlManager.getSharableUrl(url, containerName)
  }


}
