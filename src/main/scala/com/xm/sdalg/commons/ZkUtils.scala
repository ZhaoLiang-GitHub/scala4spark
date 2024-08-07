package com.xm.sdalg.commons

import com.xm.miliao.zookeeper._
import com.xm.sdalg.commons.ClusterUtils._

object ZkUtils {
    def zkEnvTypeMap(locale: String): String = {
        if (localeInCluster(locale, singapore))
            "alsgcloudsrv"
        else if (localeInCluster(locale, india))
            "azmbcloudsrv"
        else if (localeInCluster(locale, russia))
            "ksmoscloudsrv"
        else if (localeInCluster(locale, germany))
            "azdecloudsrv"
        else if (localeInCluster(locale, holland))
            "azamssrv"
        else if (locale == "C3")
            "c3"
        else if (locale == "c4")
            "c4"
        else
            "alsgcloudsrv"
    }

    /**
     * 从给定的zk路径中获得配置的内容
     * 返回的是zk界面的全部的字符串数据
     */
    def getDataFromZkPath(path: String, locale: String): String = {
        ZKFacade
            .getClient(EnvironmentType.valueOf(zkEnvTypeMap(locale)))
            .getData(classOf[String], path)
    }

    def writeData2ZkPath(path: String, input: String): Boolean = {
        if (!ZKFacade.getAbsolutePathClient.exists(path))
            ZKFacade.getAbsolutePathClient.createPersistent(path)
        ZKFacade.getAbsolutePathClient.updatePersistent(path, input)
    }
}
