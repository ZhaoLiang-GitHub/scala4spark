package com.xm.sdalg.appstrore_recommend

import com.google.gson.JsonParser
import com.xm.data.appstore.sdalg.recommend.GetAppsRecommendSrvNewLog
import com.xm.data.commons.spark.HdfsIO.SparkContextThriftFileWrapper
import com.xm.sdalg.commons.ClusterUtils._
import com.xm.sdalg.commons.CubeUtils._
import com.xm.sdalg.commons.HDFSUtils
import com.xm.sdalg.commons.HDFSUtils._
import com.xm.sdalg.commons.TimeUtils._
import com.xm.sdalg.commons.ZkUtils._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType}

import scala.collection.mutable.ArrayBuffer

object Utils extends Serializable {
    // 数鲸平台的abtest数据
    val dwm_mipicks_recommend_di = "/user/h_data_platform/platform/dw/dwm_mipicks_recommend_di"
    // 商店大盘全局实验统计数据
    val dwm_mipicks_recommend_dapan_di = "/user/h_data_platform/platform/dw/dwm_mipicks_recommend_dapan_di"
    // 用户搜索明细表
    val dwd_appstore_search_di = "/user/h_data_platform/platform/dw/dwd_appstore_search_di"
    // 基于swing的i2i
    val dwd_appstore_recommend_swing_i2i_1d = "/user/h_data_platform/platform/appstore/dwd_appstore_recommend_swing_i2i_1d"
    // 用户分层
    val dwd_appstore_recommend_user_hierarchical_1d = "/user/h_data_platform/platform/appstore/dwd_appstore_recommend_user_hierarchical_1d"
    // 用户离线行为表，通过tag来做区分
    val dwd_appstore_recommend_user_offline_behavior_1d = "/user/h_data_platform/platform/appstore/dwd_appstore_recommend_user_offline_behavior_1d"
    // 用户离线行为聚合表
    val dwd_appstore_recommend_user_behavior_1d = "/user/h_data_platform/platform/appstore/dwd_appstore_recommend_user_behavior_1d"
    // 应用过滤数据
    val dwd_appstore_recommend_app_uninstall_rate = "/user/h_data_platform/platform/appstore/dwd_appstore_recommend_app_uninstall_rate/ID"

    // 召回日志落盘表
    def appstore_mipicks_recommend_srv_log(date: String) = s"/user/s_lcs/appstore/appstore_mipicks_recommend_srv_log${date2yyyyMMdd(date)}"

    // i2i 聚合结果
    def dwd_appstore_recommend_i2i_1d(date: String, locale: String): String = s"/user/h_data_platform/platform/appstore/dwd_appstore_recommend_i2i_1d/locale=$locale/date=$date"

    /**
     * 印尼+东南亚5国+土耳其的tag doc的明文内容
     */
    def dim_appstore_recommend_tag_doc(date: String, locale: String, cate: String): String = s"/user/h_data_platform/platform/appstore/dim_appstore_recommend_tag_doc/locale=$locale/cate=$cate/date=$date"

    /**
     * 旧GP 类别
     */
    def package2intlCategoryId(sparkSession: SparkSession, locale: String): DataFrame = {
        val date = getLastIcebergDate(sparkSession, s"${locale2catalog(locale, "iceberg")}.appstore.dwd_ias_recommend_app_di")
        sparkSession.table(s"${locale2catalog(locale, "iceberg")}.appstore.dwd_ias_recommend_app_di")
            .where(filterLocale("locale", locale))
            .where(s"date = '${date}' ")
            .selectExpr("packagename as package_name", "intlcategoryid as intl_category_id")
    }

    /**
     * 兼容读取 dwm_appstore_user_behavior_di
     */
    def readParquet4dwm_appstore_user_behavior_di(sparkSession: SparkSession, locale: String, startDate: String, endDate: String): DataFrame = {
        val database =
            if (localeInCluster(locale, russia)) "dw"
            else if (localeInCluster(locale, germany)) "dw"
            else "dwm"
        readTableInAlphaBetweenDate(sparkSession, locale2catalog(locale, "hive"), database, "dwm_appstore_user_behavior_di", startDate, endDate)
    }

    /**
     * 兼容读取原始日志表
     */
    def readParquet4dwd_appstore_client_track(sparkSession: SparkSession, startDate: String, endDate: String, locale: String): DataFrame = {
        readTableInAlphaBetweenDate(sparkSession, locale2catalog(locale, "hive"), "dw", "dwd_appstore_client_track", startDate, endDate)
    }

    /**
     * 兼容读取 dwd_app_stat_event_di
     */
    def readParquet4dwd_app_stat_event_di(sparkSession: SparkSession, startDate: String, endDate: String, locale: String): DataFrame = {
        if (localeInCluster(locale, germany))
            readTableInAlphaBetweenDate(sparkSession, locale2catalog(locale, "hive"), "appstore", "dwd_app_stat_event_di", startDate, endDate)
        else
            readTableInAlphaBetweenDate(sparkSession, locale2catalog(locale, "hive"), "dw", "dwd_app_stat_event_di", startDate, endDate)
    }

    /**
     * 兼容读取 dwm_app_usage_di
     */
    def readParquet4dwm_app_usage_di(sparkSession: SparkSession, startDate: String, endDate: String, locale: String): DataFrame = {
        if (localeInCluster(locale, germany))
            readTableInAlphaBetweenDate(sparkSession, locale2catalog(locale, "hive"), "appstore", "dwm_app_usage_di", startDate, endDate)
        else
            readTableInAlphaBetweenDate(sparkSession, locale2catalog(locale, "hive"), "dwm", "dwm_app_usage_di", startDate, endDate)
    }

    /**
     * gaid 做过滤不包含null 和 默认值
     */
    def filterGaid(gaid: String): String = {
        s"""
           |${gaid} is not null and $gaid not in ('','00000000-0000-0000-0000-000000000000')
           |""".stripMargin
    }


    /**
     * 获得除热门召回通道以外的通道的不可召回结果
     * 热门召回每个用户都会有，所以不必在召回
     */
    def getRecallBlackSet(sparkSession: SparkSession, locale: String, date: String, hotType: String = "all_hot", topN: Int = 200): Broadcast[Set[String]] = {
        val path = getLatestExistPath(sparkSession, dim_appstore_recommend_tag_doc("yyyyMMdd", locale2locale(locale), "hotItemByOldWay"), date, 365)
        if (path == null)
            return sparkSession.sparkContext.broadcast(Set[String]())
        val org: RDD[(String, Map[String, Long])] = getMapFromDocPath[Long](sparkSession, locale, path, key = "id", value = "apps", blackKeySet = null, whiteKeySet = null, dataType = "Long")
        var map: collection.Map[String, Set[String]] = org
            .map(row => (row._1, row._2))
            .mapValues(_.toArray.sortBy(_._2).reverse.slice(0, topN).map(_._1).toSet)
            .collectAsMap()
        var version: String = null
        version =
            if (locale == "ID") "SAB_CD_V7"
            else if (locale == "IN") "SAB_C_V7"
            else ""
        map ++= Map(
            "all_hot" ->
                map.getOrElse(s"hotAppsInRec$version", Set())
                    .++(map.getOrElse(s"hotGamesInRec$version", Set()))
                    .++(map.getOrElse(s"topAppsInSearch$version", Set()))
        ) // 全部的热门结果
        sparkSession.sparkContext.broadcast(map(hotType))
    }

    // 获得业务标签黑名单
    def getAuditBlackSet(sparkSession: SparkSession, locale: String,
                         auditTagIdSet: Set[String] = Set("86", "101", "92", "105")): Broadcast[Set[String]] = {
        // 20230809 从 s"audit_tag_name in ('擦边应用')" 更改到 s"audit_tag_id in (86)
        val org = sparkSession.read.table(s"${locale2catalog(locale, "iceberg")}.appstore.dwd_ias_app_tag_df")
            .where(s"date between ${getFrontDay(getCurrentDateStr(), 7)} and ${getCurrentDateStr()}")
            .where(s"audit_tag_id in (${auditTagIdSet.map(i => s"\'$i\'").mkString(",")})")
            .selectExpr("audit_tag_id", "package_name")
            .rdd
            .map(row => (row.getLong(0), row.getString(1)))
            .groupByKey()
            .map(row => (row._1, row._2.toSet))
            .collect()
        val auditTagName: Broadcast[Set[String]] = sparkSession.sparkContext.broadcast(org.flatMap(_._2).toSet)
        println(
            s"""
               |过滤的业务侧黑名单原则是audit_tag_id in (${auditTagIdSet.map(i => s"\'$i\'").mkString(",")})
               |其中每个类别需要被过滤的数量是${org.map(row => (row._1, row._2.size)).mkString(",")}
               |过滤业务侧黑名单总数量是${auditTagName.value.size}
               |""".stripMargin)
        if (auditTagName.value.isEmpty) {
            throw new Exception("业务侧黑名单为空，程序退出")
        }
        auditTagName
    }

    /**
     * 基于新表的可推荐内容池
     */
    def getAppSetBc(sparkSession: SparkSession, locale: String, endDate: String,
                    front7DayLevel: String, front1DayLevel: String,
                    cate: String = "all",
                    version: String = "none"): Broadcast[Set[String]] = {
        println(
            s"""
               |--------------------------------------------------------------------------------------------------
               |当前的参数是 : getAppSetBc ${locale} ${endDate} ${front7DayLevel} ${front1DayLevel} ${cate} ${version}
               |--------------------------------------------------------------------------------------------------
               |""".stripMargin)

        val date = getLastIcebergDate(sparkSession, s"${locale2catalog(locale, "iceberg")}.appstore.dwd_ias_recommend_app_di", endDate)
        var orgDf: DataFrame = sparkSession.table(s"${locale2catalog(locale, "iceberg")}.appstore.dwd_ias_recommend_app_di")
            .where(filterLocale("locale", locale))
            .where(s"date between ${getFrontDay(date, 8)} and ${date}")
            .selectExpr("packagename", "level", "intlcategoryid", "gpscore", "displayscore")
        cate match {
            case "game" => orgDf = orgDf.where("intlcategoryid >= 100")
            case "app" => orgDf = orgDf.where("intlcategoryid < 100")
            case "all" =>
        }
        val front1Day = orgDf
            .where(s"date = '${date}'")
            .selectExpr("packagename", "level")
            .rdd
            .map(row => (row.getString(0), row.getString(1)))
            .filter({ case (_, level) => levelSplit(front1DayLevel).contains(level) })
        val front7Day = orgDf
            .where(s"date between ${getFrontDay(date, 8)} and ${getFrontDay(date, 1)}")
            .selectExpr("packagename", "level")
            .rdd
            .map(row => (row.getString(0), row.getString(1)))
            .filter({ case (_, level) => levelSplit(front7DayLevel).contains(level) })
        var result = sparkSession.sparkContext.broadcast(
            front7Day
                .union(front1Day)
                .map(_._1)
                .distinct()
                .collect()
                .toSet
        )

        {
            if (locale == "ID") {
                // 20230922 过滤CD级的小时卸载率大于0.7实验推全
                val filterApp = sparkSession.read.parquet(dwd_appstore_recommend_app_uninstall_rate)
                    .where("level in ('C','D')")
                    .where("hour_uninstall_rate_filter > 0.7")
                    .selectExpr("pkg").rdd.map(_.getString(0)).collect().toSet
                println(s"过滤参数是${version} ,过滤掉的app有 " + filterApp.size + " 个")
                result = sparkSession.sparkContext.broadcast(result.value.diff(filterApp))
            }
        }

        /**
         * 20230911 线上过滤策略迁移至线下
         */
        {
            // 日活过滤
            if (locale == "ID") {
                val lastDate = getLastHiveDate(sparkSession, s"${locale2catalog(locale, "hive")}.appstore.ads_appstore_app_quality_score_global_1d_test")
                val temp = sparkSession.sql(
                    s"""
                       |-- 未屏蔽的app
                       |SELECT
                       |    distinct pkg
                       |FROM
                       |    ${locale2catalog(locale, "hive")}.appstore.ads_appstore_app_quality_score_global_1d_test
                       |where
                       |    date = ${lastDate}
                       |    and lo in ('ID')
                       |    and status=200
                       |    and level not in ('E')
                       |    and (
                       |        act_retain>=0.05  -- 未屏蔽的app，要么次留大于等于5%
                       |        or avg_daily_usage_7d>=1  -- 要么周均日活大于等于1
                       |    )
                       |""".stripMargin)
                    .rdd
                    .map(_.getString(0))
                    .collect()
                    .toSet
                println("在印尼：根据次留大于等于5%或者周均日活大于等于1，可推荐的app个数是 " + temp.size)
                result = sparkSession.sparkContext.broadcast(result.value.intersect(temp))
            }
        }
        {
            // 黑名单过滤
            val blackAppSetV2InZk = getDataFromZkPath("/misearch/recommend/appstore/config/black_list_v2.properties", locale)
                .split("\n")
                .map(_.split("\t"))
                .filter(_.length == 2)
                .map(_ (0))
                .toSet
            println("当前线上zk黑名单个数是 " + blackAppSetV2InZk.size)
            result = sparkSession.sparkContext.broadcast(result.value.diff(blackAppSetV2InZk))
        }
        {
            // 色情应用过滤
            val auditTagName = getAuditBlackSet(sparkSession, locale)
            result = sparkSession.sparkContext.broadcast(result.value.diff(auditTagName.value))
        }
        {
            // gp分数大于2.0
            val gp2 = orgDf
                .where("gpscore >= 2.0")
                .selectExpr("packagename")
                .rdd
                .map(_.getString(0))
                .collect()
                .toSet
            println("gp分数大于2.0的可推荐的app个数是 " + gp2.size)
            result = sparkSession.sparkContext.broadcast(result.value.intersect(gp2))
            // 外显分大于2.0
            val displayscore2 = orgDf
                .where("displayscore >= 2.0")
                .selectExpr("packagename")
                .rdd
                .map(_.getString(0))
                .collect()
                .toSet
            println("外显分大于2.0的可推荐的app个数是 " + displayscore2.size)
            result = sparkSession.sparkContext.broadcast(result.value.intersect(displayscore2))
        }
        println(
            s"""
               |--------------------------------------------------------------------------------------------------
               |实际读取 ${locale2catalog(locale, "iceberg")}.appstore.dwd_ias_recommend_app_di 最新的日期是 ${date}
               |--------------------------------------------------------------------------------------------------
               |前7天的数据总量是 ${front7Day.map(_._1).distinct().count()}
               |分级别的数据量是 ${
                front7Day
                    .groupBy(_._2)
                    .map({ case (level, pkgs) => (level, pkgs.toSet.size) })
                    .collect()
                    .mkString(" ")
            }
               |--------------------------------------------------------------------------------------------------
               |前1天的数据总量是 ${front1Day.map(_._1).distinct().count()}
               |分级别的数据量是 ${
                front1Day
                    .groupBy(_._2)
                    .map({ case (level, pkgs) => (level, pkgs.toSet.size) })
                    .collect()
                    .mkString(" ")
            }
               |--------------------------------------------------------------------------------------------------
               |经过过滤层之后的最后的数量 ${result.value.size}
               |--------------------------------------------------------------------------------------------------
               |""".stripMargin)
        result
    }

    /**
     * 获得最新一天的包、级别
     */
    def getLatestAppSetBC(sparkSession: SparkSession, locale: String, level: String = "SABCD"): DataFrame = {
        val date = getLastIcebergDate(sparkSession, s"${locale2catalog(locale, "iceberg")}.appstore.dwd_ias_recommend_app_di")
        sparkSession.table(s"${locale2catalog(locale, "iceberg")}.appstore.dwd_ias_recommend_app_di")
            .where(filterLocale("locale", locale))
            .where(s"date = '${date}' ")
            .selectExpr("packagename as pkg", "level", "appid", "tagids")
            .where("level in ('" + level.split("").mkString("','") + "')")
    }

    /**
     * 应用到标签的映射
     */
    def getPackage2Tags(sparkSession: SparkSession,
                        locale: String,
                        minNum: Int = 100,
                        rate: Double = 0.005): DataFrame = {
        val org = getLatestAppSetBC(sparkSession, locale)
            .withColumn("tagid", explode(from_json(col("tagids"), ArrayType(StringType))))
            .where("tagid is not null")
            .selectExpr("pkg", "cast(tagid as int) as tagid")
            .distinct()
        val tagFilterV1 = org
            .groupBy("tagid").count()
            .where(s"count >= ${minNum}") // 2. 标签下SABC级应用数量≥100个
            .selectExpr("tagid")

		val lo_condition = filterLocale("lo", locale)

        val tagFilterV2 = sparkSession.sql(
            s"""
               |WITH
               |    raw_track as (
               |        SELECT
               |            gaid,
               |            pre_page_type,
               |            pre_card_type,
               |            cur_page_type,
               |            cur_card_type,
               |            package_name,
               |            ext_rec,
               |            action_type,
               |            install_type,
               |            install_status,
               |            cur_page_refs,
               |            cur_page_ref,
               |            cur_card_pos,
               |            cur_item_pos,
               |            if (
               |                split(ext_rec, '_') [0] IS NOT NULL,
               |                if (
               |                    split(ext_rec, '_') [0] LIKE '%;%',
               |                    split(split(ext_rec, '_') [0], ';') [1],
               |                    split(ext_rec, '_') [0]
               |                ),
               |                NULL
               |            ) AS exp_date,
               |            if (
               |                split(ext_rec, '_') [5] IS NOT NULL,
               |                if (
               |                    split(split(ext_rec, '_') [5], '@') [3] IS NOT NULL,
               |                    if (
               |                        split(split(split(ext_rec, '_') [5], '@') [3], '#') [0] IS NOT NULL,
               |                        split(
               |                            split(split(split(ext_rec, '_') [5], '@') [3], '#') [0],
               |                            ',|，'
               |                        ),
               |                        NULL
               |                    ),
               |                    NULL
               |                ),
               |                NULL
               |            ) AS model_id,
               |            app_id AS appId,
               |            date,
               |            time AS download_time,
               |            hour(from_unixtime(time/1000, 'yyyy-MM-dd HH:mm:ss')) AS download_hour
               |        FROM
               |            ${locale2catalog(locale, "hive")}.dw.dwd_appstore_client_track
               |        WHERE
               |            date BETWEEN ${getFrontDay(30)} AND ${getFrontDay(2)}
               |            AND ${lo_condition}
               |            AND market_v>=4002000
               |            AND ext_rec IS NOT NULL
               |            AND gaid IS NOT NULL
               |    ),
               |    exposure_data AS (
               |        SELECT
               |            date,
               |            package_name,
               |            count(
               |                DISTINCT if (
               |                    cur_page_type IN (
               |                        'recommend',
               |                        'client_homepage',
               |                        'native_home_feature'
               |                    )
               |                    AND action_type='item_exposure',
               |                    gaid,
               |                    NULL
               |                )
               |            ) AS exposure_uv
               |        FROM
               |            (
               |                SELECT
               |                    gaid,
               |                    date,
               |                    cur_page_type,
               |                    action_type,
               |                    package_name,
               |                    explode (model_id) as model_id
               |                FROM
               |                    raw_track
               |                -- WHERE
               |                    -- raw_track.exp_date=raw_track.date
               |            )
               |        GROUP BY
               |            date,
               |            package_name
               |    ),
               |    download_data AS (
               |        SELECT
               |            date,
               |            package_name,
               |            count(DISTINCT concat(gaid, package_name)) AS download_puv
               |        FROM
               |            (
               |                SELECT
               |                    gaid,
               |                    date,
               |                    package_name,
               |                    download_time,
               |                    download_hour
               |                FROM
               |                    raw_track
               |                WHERE
               |                    cur_page_type in (
               |                        'recommend',
               |                        'client_homepage',
               |                        'native_home_feature'
               |                    )
               |                    AND cur_card_type='recApps'
               |                    AND action_type='download_install'
               |                    AND install_status='download_request'
               |                    AND install_type in ('manual_install', 'auto_install')
               |                    -- and exp_date=date
               |                UNION ALL
               |                SELECT
               |                    gaid,
               |                    date,
               |                    package_name,
               |                    download_time,
               |                    download_hour
               |                FROM
               |                    raw_track
               |                WHERE
               |                    cur_page_type like '%detail%'
               |                    AND pre_page_type in (
               |                        'recommend',
               |                        'client_homepage',
               |                        'native_home_feature'
               |                    )
               |                    AND pre_card_type='recApps'
               |                    AND action_type='download_install'
               |                    AND install_status='download_request'
               |                    AND install_type in ('manual_install', 'auto_install')
               |                    -- and raw_track.exp_date=raw_track.date
               |            )
               |        GROUP BY
               |            date,
               |            package_name
               |    )
               |SELECT
               |    exposure_data.package_name as pkg,
               |    sum(exposure_uv) as exposure_uv,
               |    sum(download_puv) as download_puv
               |FROM
               |    exposure_data
               |    JOIN download_data ON exposure_data.package_name=download_data.package_name
               |    AND exposure_data.date=download_data.date
               |group by
               |    exposure_data.package_name
               |""".stripMargin)
            .join(org, Seq("pkg"), "inner")
            .selectExpr("tagid", "exposure_uv", "download_puv")
            .groupBy("tagid").agg(sum("exposure_uv").as("exposure_uv"), sum("download_puv").as("download_puv"))
            .withColumn("rate", expr("download_puv/exposure_uv"))
            .where(s"rate >= ${rate}") //  3. 带有标签的应用在首页首页下载人包/首页元素曝光uv ≥ 0.5%"

        val res = org
            .join(tagFilterV1, Seq("tagid"))
            .join(tagFilterV2, Seq("tagid"))
            .selectExpr("pkg", "tagid")
            .distinct()
        println(
            s"""
               |原有的tag数量是 ${org.selectExpr("tagid").distinct().count()}
               |原有的应用数是 ${org.selectExpr("pkg").distinct().count()}
               |原有的tag下的应用数是 ${org.rdd.map(r => (r.getInt(1), 1)).reduceByKey(_ + _).collect().sortBy(_._1).mkString(",")}
               |过滤条件是
               |    1. 每个标签在SABCD的应用数 >= ${minNum}
               |    2. 每个标签在首页下载puv/首页曝光uv >= ${rate}
               |过滤后的结果是
               |共有tag数量 ${res.selectExpr("tagid").distinct().count()}
               |共有应用数 ${res.selectExpr("pkg").distinct().count()}
               |每个tag下的数据量分布 ${res.rdd.map(r => (r.getInt(1), 1)).reduceByKey(_ + _).collect().sortBy(_._1).mkString(",")}
               |""".stripMargin)
        res
    }

    /**
     * 根据搜索的下载数量
     */
    def getDownloadNumBySearch(sparkSession: SparkSession,
                               locale: String,
                               startDate: String,
                               endDate: String,
                               name: String,
                               whiteSet: Broadcast[Set[String]],
                               blackSet: Broadcast[Set[String]]): DataFrame = {
        readParquet4dwd_app_stat_event_di(sparkSession, startDate, endDate, locale)
            .where(s"event_type in ('first_launch')")
            .where(filterLocale("region", locale))
            .filter(row => whiteSet.value.contains(row.getAs[String]("package_name")))
            .filter(row => !blackSet.value.contains(row.getAs[String]("package_name")))
            .selectExpr("imeimd5", "package_name")
            .groupBy("package_name").agg(countDistinct("imeimd5").as(name))
    }

    /**
     * 指定槽位，指定实验分组，指定召回队列下，只在这个队列下的召回数据的结果
     */
    def getRecallDf(sparkSession: SparkSession,
                    locale: String,
                    startDate: String,
                    endDate: String,
                    slot: Array[Int] = Array(2),
                    eid: String = null,
                    only1queue: String = null): DataFrame = {
        import sparkSession.implicits._
        var orgDf: DataFrame = null
        dateSeqBetweenStartAndEnd(startDate, endDate).foreach(date => {
            if (HDFSUtils.exists(sparkSession.sparkContext, appstore_mipicks_recommend_srv_log(date))) {
                val temp = sparkSession.sparkContext.thriftSeqDataFrame(appstore_mipicks_recommend_srv_log(date), classOf[GetAppsRecommendSrvNewLog])
                if (orgDf == null) orgDf = temp
                else orgDf = orgDf.union(temp)
            }
        })
        orgDf
            .where(
                s"""
                   |source = 'recall'
                   |and app = 'getAppsID'
                   |and environment = 'sgp'
                   |and level = 'info'
                   |and slot in (${slot.map(i => "\'" + i + "\'").mkString(",")})
                   |""".stripMargin)
            .rdd
            .map(row => {
                val eid = row.getAs[String]("mark")
                val gaid = row.getAs[String]("gaid")
                val message = new JsonParser().parse(row.getAs[String]("message")).getAsJsonArray
                val packages = new ArrayBuffer[String]()
                Range(0, message.size()).foreach(i => {
                    packages.append(message.get(i).getAsString)
                })
                val queue = row.getAs[String]("group")
                val timestamp = row.getAs[Long]("timestamp").toString
                (gaid, eid, queue, packages, timestamp)
            })
            .flatMap(row => {
                row._4.map(ii => (row._1, row._2, row._3, ii, row._5))
            })
            .toDF("gaid", "eid", "queue", "pkg", "timestamp")
            .filter(row => {
                var flag = true
                if (eid != null)
                    flag = row.getAs[String]("eid").contains(eid)
                flag
            })
            .rdd
            .map(row => ((row.getString(0), row.getString(1), row.getString(3), row.getString(4)), row.getString(2)))
            .groupByKey()
            .mapValues(_.toArray.mkString(","))
            .map(row => (row._1._1, row._1._2, row._1._3, row._1._4, row._2))
            .toDF("gaid", "eid_all", "pkg", "timestamp", "queue_all") // 一个用户一次请求时间戳一样。这个用户在这个组内在不同通道内都请求了这个包
            .filter(row => {
                // 是否只过滤 只通过这一个通道的召回结果
                var flag = true
                if (only1queue != null) {
                    val queue_all = row.getAs[String]("queue_all")
                    flag = queue_all.split(",").length == 1 && queue_all.split(",")(0) == only1queue
                }
                flag
            })
    }

}
