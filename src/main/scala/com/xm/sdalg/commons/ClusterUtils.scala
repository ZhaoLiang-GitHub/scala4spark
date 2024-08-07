package com.xm.sdalg.commons

/**
 * 数据集群与国家之间的关系
 */
object ClusterUtils extends Serializable {
    // 不同集群下的国家
    val SG_OTHER: Set[String] = Set("SG_OTHER", "BR", "TR", "MX", "CO", "BD", "VN", "EG", "PE", "PH", "DZ", "MY", "TH", "MM", "CL", "MA", "IQ", "EC", "KG", "PK", "SA", "AR", "AE", "JP", "UY", "MD", "PY", "QA", "TN", "DO", "GT", "ZA", "SG", "OM", "PA", "CR", "KE", "JO", "SV", "KW", "CI", "BH", "TZ", "PG", "NZ", "SO", "UZ", "VE", "KZ", "NP", "BY", "AZ", "IL", "BO", "AF", "YE", "CU", "NG", "LK", "NI", "AM", "TM", "PF", "LY", "AS", "TJ", "AU", "AO", "KH", "KR", "SD", "HN", "AW", "CA", "AI", "AG", "CM", "MV", "SN") // 新加坡集群中全部国家
    val singapore: Set[String] = SG_OTHER + "ID" + "SG_ALL" // 新加坡 新加坡集群
    val india: Set[String] = Set("IN") // 印度 孟买集群
    val russia: Set[String] = Set("RU") // 俄罗斯 莫斯科集群
    val germany: Set[String] = Set("GERMANY", "ES", "FR", "IT", "DE", "PL", "GB", "UA", "RO", "AT", "CH", "IE", "GR", "CZ", "PT", "HU", "AL", "RS", "BG", "SK", "HR", "GE", "BA", "AD", "FI", "LV", "SI", "MK", "EE", "CY", "ME") // 德国 法兰克福集群
    val holland: Set[String] = Set("DUTCH") // 荷兰 阿姆斯特丹集群

    /**
     * 对输入的locale参数做判断
     * 当包含了逗号时则是输入了多个国家返回一个统一的名字
     * 不包含逗号时是单一国家
     */
    def locale2locale(locale: String): String =
        if (locale.contains(",") || locale == "SG_OTHER")
            "OTHER"
        else
            locale

    /**
     * 适配多国的 locale过滤方式
     * 输入的locale 是可能含有逗号的多个国家字符串：MY,TR,VN
     */
    def filterLocale(fileName: String, locale: String): String =
        if (locale == "SG_ALL") // 新加坡集群全部国家
            s"$fileName in (${singapore.map(i => s"'$i'").mkString(",")})"
        else if (localeInCluster(locale, SG_OTHER) || locale == "SG_OTHER") // 新加坡集群其他国家
            s"$fileName in (${SG_OTHER.map(i => s"'$i'").mkString(",")})"
        else if (localeInCluster(locale, germany) || locale == "GERMANY") // 德国集群
            s"$fileName in (${germany.map(i => s"'$i'").mkString(",")})"
        else if (!locale.contains(",")) // 单个国家
            s"$fileName = '$locale'"
        else
            s"""
               |$fileName in (${locale.split(",").map(i => s"'$i'").mkString(",")})
               |""".stripMargin


    /**
     * 输入的locale是否属于当前集群下的国家列表
     * 一个国家(不包含逗号)、多个国家(包括逗号)
     */
    def localeInCluster(locale: String, cluster: Set[String]): Boolean =
        cluster.contains(locale) || locale.split(",").exists(cluster.contains)

    // 适配 S,A,B  SAB
    def levelSplit(level: String): Set[String] = {
        level.split("").flatMap(_.split(",")).toSet
    }

    // 从locale得到对应的国家名字
    def locale2countryName(locale: String): String = {
        if (!locale.contains(",")) {
            Map("ID" -> "印尼", "IN" -> "印度")(locale)
        } else {
            if (localeInCluster(locale, singapore))
                "新加坡集群其他国家"
            else
                ""
        }
    }

    /**
     * 每个国家的不同集群的参数
     */
    def locale2catalog(locale: String, catalogType: String): String = {
        val map = Map(
            singapore -> Map("hive" -> "hive_alsgprc_hadoop", "iceberg" -> "iceberg_alsgprc_hadoop", "hadoop" -> "alsgprc-hadoop"),
            india -> Map("hive" -> "hive_azmbcommonprc_hadoop", "iceberg" -> "iceberg_azmbcommonprc_hadoop", "hadoop" -> "azmbcommonprc-hadoop"),
            russia -> Map("hive" -> "hive_ksmosprc_hadoop", "iceberg" -> "iceberg_ksmosprc_hadoop", "hadoop" -> "ksmosprc-hadoop"),
            holland -> Map("hive" -> "hive_azamsprc_hadoop", "iceberg" -> "iceberg_azamsprc_hadoop", "hadoop" -> "azamsprc-hadoop"),
            germany -> Map("hive" -> "hive_azdeprc_hadoop", "iceberg" -> "iceberg_azdeprc_hadoop", "hadoop" -> "azdeprc-hadoop")
        )
        map.collectFirst {
            case (cluster, catalogs) if localeInCluster(locale, cluster) => catalogs(catalogType)
        }.getOrElse("")
    }

}
