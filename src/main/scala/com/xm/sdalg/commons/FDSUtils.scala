package com.xm.sdalg.commons

import com.xm.infra.galaxy.fds.model.HttpMethod

import java.net.{URI, URL}
import java.util.Date
import com.xm.infra.galaxy.fds.client._
import com.xm.infra.galaxy.fds.client.credential.BasicFDSCredential
import com.xm.infra.galaxy.fds.client.FDSClientConfiguration
import org.apache.commons.mail.EmailAttachment
import org.apache.spark.sql.SparkSession

object FDSUtils {
    // CI56930用户组的账号密钥
    // 在团队管理 查询对应团队的账号和密钥 https://cloud.mioffice.cn/#/services/user-manage/org-detail/sdalg
    val CI56930_accessId = "AKWRXIS66IJR34KDD3"
    val CI56930_accessSecret = "ECYV0ImaflB55h6yWb/ziOgJnjYLkvwheANZkP2v"
    // 使用的团队账号需要在对应的bucket上有权限 https://cloud.mioffice.cn/#/product/file-store/buckets?_k=tywr0j
    val quickAppAlgoBucket = "quick-app-algo-bucket"

    // region(地域) endpoint(访问域名)  http://docs.api.xm.net/fds/basic-concept.html
    val alsgp0_endpoint_in = "alsgp0-fds.api.xm.net" // 新加坡集群FDS内网访问节点
    val alsgp0_endpoint_out = "alsgp0.fds.api.xm.com" // 新加坡集群FDS外网访问节点
    val alsgp0_region = "alsgp0"

    /**
     * 获得指定路径的FDS访问URL
     *
     * @param accessId     团队账号密钥id
     * @param accessSecret 团队账号密码
     * @param endpoint     访问域名
     * @param bucketName   bucket名称
     * @param objectName   文件名称
     */
    def getUrlFromFds(accessId: String,
                      accessSecret: String,
                      endpoint: String,
                      bucketName: String,
                      objectName: String
                     ): URI = {
        // 使用java client 链接FDS
        // SDK示例代码见 http://docs.api.xm.net/fds/sdks.html、https://github.com/xm/galaxy-fds-sdk-java
        val credential = new BasicFDSCredential(accessId, accessSecret)
        val fdsConfig = new FDSClientConfiguration(endpoint)
        val client = new GalaxyFDSClient(credential, fdsConfig)
        val expiration = new Date(System.currentTimeMillis() + 1000 * 60 * 60 * 24 * 365) // rul过期时间
        val uri: URI = client.generatePresignedUri(bucketName, objectName, expiration, HttpMethod.GET)
        uri
    }

}
