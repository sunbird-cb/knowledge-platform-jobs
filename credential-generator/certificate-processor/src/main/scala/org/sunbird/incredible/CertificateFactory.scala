package org.sunbird.incredible

import java.io.IOException
import java.time.Instant
import java.util.UUID

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.lang.StringUtils
import org.slf4j.{Logger, LoggerFactory}
import org.sunbird.incredible.pojos.ob.{BadgeClass, CertificateExtension, CompositeIdentityObject, Issuer, Signature, SignedVerification, TrainingEvidence}
import org.sunbird.incredible.processor.CertModel
import org.sunbird.incredible.processor.signature.SignatureHelper

object CertificateFactory {

  private val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  lazy private val mapper: ObjectMapper = new ObjectMapper()
  mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)

  def createCertificate(certModel: CertModel)(implicit certificateConfig: CertificateConfig): CertificateExtension = {
    val basePath = getDomainUrl(certificateConfig.basePath, certModel.tag)
    val uuid: String = basePath + "/" + UUID.randomUUID.toString

    val compositeIdentity: CompositeIdentityObject = CompositeIdentityObject(context = certificateConfig.contextUrl,
      identity = certModel.identifier,
      name = certModel.recipientName,
      hashed = false,
      `type` = Array(JsonKeys.ID))

    val issuer: Issuer = Issuer(context = certificateConfig.contextUrl,
      id = Option.apply(certificateConfig.issuerUrl),
      name = certModel.issuer.name,
      url = certModel.issuer.url,
      publicKey = certModel.issuer.publicKey)

    val badgeClass: BadgeClass = BadgeClass(certificateConfig.contextUrl,
      id = basePath.concat(if (StringUtils.isNotBlank(certModel.tag)) "/".concat(certModel.tag).concat(JsonKeys.BADGE_URL) else JsonKeys.BADGE_URL),
      description = certModel.certificateDescription.orNull,
      name = if (StringUtils.isNotEmpty(certModel.courseName)) certModel.courseName else certModel.certificateName,
      image = certModel.certificateLogo.orNull,
      issuer = issuer,
      criteria = certModel.criteria)

    val certificateExtension: CertificateExtension = CertificateExtension(certificateConfig.contextUrl,
      id = uuid, recipient = compositeIdentity,
      badge = badgeClass,
      issuedOn = certModel.issuedDate,
      expires = certModel.expiry.orNull,
      validFrom = certModel.validFrom.orNull,
      signatory = certModel.signatoryList,
      providerName = certModel.providerName)
    if (StringUtils.isNotEmpty(certModel.courseName)) {
      val trainingEvidence: TrainingEvidence = TrainingEvidence(certificateConfig.contextUrl,
        id = certificateConfig.evidenceUrl,
        name = certModel.courseName,
        extendedName=certModel.courseExtendedName)
      certificateExtension.evidence = Option.apply(trainingEvidence)
    }

    var signedVerification: SignedVerification = null
    if (StringUtils.isEmpty(certModel.keyId)) {
      signedVerification = SignedVerification(`type` = Array(JsonKeys.HOSTED))
      logger.info("CertificateExtension:createCertificate: if keyID is empty then verification type is HOSTED")
    } else {
      signedVerification = SignedVerification(creator = Option.apply(basePath.concat("/").concat(JsonKeys.KEYS).concat("/").concat(certModel.keyId).concat(JsonKeys.PUBLIC_KEY_URL)))
      logger.info("CertificateExtension:createCertificate: if keyID is not empty then verification type is SignedBadge")
      val signatureValue = getSignatureValue(certificateExtension, certificateConfig.encryptionServiceUrl, certModel.keyId)
      val signature: Signature = Signature(created = Instant.now.toString, creator = basePath.concat("/").concat(certModel.keyId).concat(JsonKeys.PUBLIC_KEY_URL), signatureValue = signatureValue)
      certificateExtension.signature = Option.apply(signature)
    }
    certificateExtension
  }


  /**
    * to get signature value of certificate
    *
    * @return
    */
  @throws[IOException]
  private def getSignatureValue(certificateExtension: CertificateExtension, encServiceUrl: String, keyId: String): String = {
    var signMap: java.util.Map[String, AnyRef] = null
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)
    val request = mapper.writeValueAsString(certificateExtension)
    val jsonNode = mapper.readTree(request)
    logger.info("CertificateFactory:getSignatureValue:Json node of certificate".concat(jsonNode.toString))
    signMap = SignatureHelper.generateSignature(jsonNode, keyId)(encServiceUrl)
    signMap.get(JsonKeys.SIGNATURE_VALUE).asInstanceOf[String]
  }

  /**
    * appends slug , org id, batch id to the domain url
    */
  private def getDomainUrl(basePath: String, tag: String): String = {
    val subString = if (StringUtils.isNotEmpty(tag)) "/".concat(tag) else ""
    s"""$basePath$subString"""
  }

}
