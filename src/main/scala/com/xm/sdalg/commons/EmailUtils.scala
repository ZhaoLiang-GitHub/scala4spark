package com.xm.sdalg.commons

import com.xm.sdalg.commons.FDSUtils._
import org.apache.commons.mail.{EmailAttachment, MultiPartEmail, HtmlEmail}

import java.net.URL


object EmailUtils {
    /**
     * 给指定的用户发邮件
     *
     * @param recipients       收件人列表
     * @param subject          主题
     * @param message          文本内容
     * @param emailAttachments 附件
     */
    def sendEMail(recipients: Array[String],
                  subject: String,
                  message: String,
                  emailAttachments: Array[EmailAttachment] = null): Unit = {
        val email = new MultiPartEmail()
        email.setFrom("FlaskiHkV9@xm.com") // https://t.mioffice.cn/mail/#/token/myTokenList 申请token
        email.addTo(recipients: _*) //收件人
        email.setSubject(subject) // 设置邮件主题
        email.setHostName("mail.b2c.srv") // 设置邮箱服务器信息
        email.setCharset("UTF-8") // 设置邮件编码
        email.setMsg(message) // 设置邮件内容
        if (null != emailAttachments && emailAttachments.length > 0) // 附件
            emailAttachments.foreach(emailAttachment => email.attach(emailAttachment))
        email.send
    }

    /**
     * 将FDS上的一个文件，生成一个邮件附件
     *
     * @param name        邮件中展示的文件名称
     * @param description 文件类型
     * @return
     */
    def getEmailAttachment(accessId: String = CI56930_accessId,
                           accessSecret: String = CI56930_accessSecret,
                           endpoint: String = alsgp0_endpoint_in,
                           bucketName: String = quickAppAlgoBucket,
                           objectName: String,
                           name: String = "",
                           description: String = ""
                          ): EmailAttachment = {
        val ea: EmailAttachment = new EmailAttachment()
        // 参数与 getUrlFromFds 函数相同
        ea.setURL(new URL(getUrlFromFds(accessId, accessSecret, endpoint, bucketName, objectName).toString))
        ea.setName(name)
        ea.setDescription(description)
        ea
    }

}

