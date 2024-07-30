package org.sunbird.job.certutil.domain

import java.util
import java.util.UUID

import scala.collection.JavaConverters._

class Models extends Serializable {}

case class Actor(id: String, `type`: String = "User")

case class EventContext(channel: String = "in.ekstep",
                        env: String = "Course",
                        sid: String = UUID.randomUUID().toString,
                        did: String = UUID.randomUUID().toString,
                        pdata: util.Map[String, String] = Map("ver" -> "1.0", "id" -> "org.sunbird.learning.platform", "pid" -> "course-certificate-generator").asJava,
                        cdata: Array[util.Map[String, String]])


case class EData(props: Array[String], `type`: String)

case class EventObject(id: String, `type`: String, rollup: util.Map[String, String])

case class CertificateAuditEvent(eid: String = "AUDIT",
                                 ets: Long = System.currentTimeMillis(),
                                 mid: String = s"LP.AUDIT.${System.currentTimeMillis()}.${UUID.randomUUID().toString}",
                                 ver: String = "3.0",
                                 actor: Actor,
                                 context: EventContext = EventContext(
                                   cdata = Array[util.Map[String, String]]()
                                 ),
                                 `object`: EventObject,
                                 edata: EData = EData(props = Array("certificates"), `type` = "certificate-issued-svg"))

case class Certificate(id: String,
                       name: String,
                       token: String,
                       lastIssuedOn: String,
                       templateUrl: String,
                       `type`: String) {
  def this() = this("", "", "", "", "", "")
}

case class FailedEvent(errorCode: String,
                       error: String) {
  def this() = this("", "")
}

case class FailedEventMsg(jobName: String,
                          failInfo: FailedEvent) {
  def this() = this("certificate-generator", null)
}


case class UserEnrollmentData(userId: String,
                              courseId: String,
                              courseName: String,
                              templateId: String,
                              certificate: Certificate) {
  def this() = this( "", "", "", "", null)
}

case class Recipient(id: String, name: String, `type`: String)
case class Training(id: String, name: String, `type`: String, batchId: String)
case class Issuer(url: String, name: String, kid: String)
case class ActorObject(id: String = "Certificate Generator", `type`: String = "System")
case class EventObjectCourseCertificate(id: String, `type`: String = "GenerateCertificate")
case class EventContextCorseCertificate(pdata: Map[String, String] = Map("ver" -> "1.0", "id" -> "org.sunbird.learning.platform"))
case class BEJobRequestEvent(actor: ActorObject= ActorObject(),
                             eid: String = "BE_JOB_REQUEST",
                             edata: Map[String, AnyRef],
                             ets: Long = System.currentTimeMillis(),
                             context: EventContextCorseCertificate = EventContextCorseCertificate(),
                             mid: String = s"LMS.${UUID.randomUUID().toString}",
                             `object`: EventObjectCourseCertificate
                            )
