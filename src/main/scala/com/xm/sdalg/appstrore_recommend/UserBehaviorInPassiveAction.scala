package com.xm.sdalg.appstrore_recommend

import com.xm.sdalg.appstrore_recommend.Utils.{dwd_appstore_recommend_user_hierarchical_1d, dwd_appstore_recommend_user_offline_behavior_1d}
import com.xm.sdalg.commons.ClusterUtils.{filterLocale, locale2catalog, locale2locale}
import com.xm.sdalg.commons.HDFSUtils.{alterTableIPartitionInAlpha, readTableInAlphaBetweenDate}
import com.xm.sdalg.commons.SparkContextUtils.createSparkSession
import com.xm.sdalg.commons.TimeUtils.yyyyMMddSplit
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 被动渠道行为数据
 */
object UserBehaviorInPassiveAction {
    def main(args: Array[String]): Unit = {

        val Array(locale: String, startDate: String, endDate: String) = args
        val sparkSession: SparkSession = createSparkSession(this.getClass.getSimpleName)
        val passiveAction = getPassiveUser(sparkSession, locale, startDate, endDate)
        passiveAction.show(false)
        passiveAction.printSchema()
        passiveAction.write.mode("overwrite").parquet(s"$dwd_appstore_recommend_user_offline_behavior_1d/locale=${locale2locale(locale)}/tag=passiveaction/date=$endDate")
        alterTableIPartitionInAlpha(sparkSession, locale2catalog(locale, "hive"), "appstore", "dwd_appstore_recommend_user_offline_behavior_1d", Map("date" -> endDate, "locale" -> locale2locale(locale), "tag" -> "passiveaction"))

    }

    /**
     * 被动渠道数据
     */
    def getPassiveUser(sparkSession: SparkSession, locale: String, startDate: String, endDate: String): DataFrame = {
        import sparkSession.implicits._
        readTableInAlphaBetweenDate(sparkSession, locale2catalog(locale, "iceberg"), "appstore", "ods_ias_client_track", yyyyMMddSplit(startDate), yyyyMMddSplit(endDate))
            .where(filterLocale("lo", locale))
            .createOrReplaceTempView("ods_ias_client_track")
        val miuiAd = sparkSession.sql(
            """
              |select
              |    gaid,
              |    package_name
              |from
              |    ods_ias_client_track
              |where
              |    first_page_type in ('minicard', 'innerpage')
              |    and launch_pkg in (
              |        'com.miui.msa.global',
              |        'com.miui.securitycenter',
              |        'com.android.systemui',
              |        'com.mi.globalbrowser',
              |        'com.android.browser',
              |        'com.mi.android.globalminusscreen',
              |        'com.miui.cleanmaster',
              |        'com.miui.cleaner',
              |        'com.xm.discover',
              |        'com.miui.android.fashiongallery',
              |        'com.miui.gallery',
              |        'com.miui.videoplayer',
              |        'com.mi.android.globalpersonalassistant',
              |        'cn.wps.xm.abroad.lite',
              |        'com.android.thememanager',
              |        'com.xm.midrop',
              |        'com.android.calendar',
              |        'com.mi.android.globalFileexplorer',
              |        'com.miui.bugreport',
              |        'com.miui.calculator',
              |        'com.xm.bluetooth',
              |        'com.xm.calendar',
              |        'com.xm.hm.health',
              |        'com.android.providers.downloads.ui',
              |        'com.miui.player'
              |    )
              |    and (
              |        launch_ref like 'DELIVERY%'
              |        or launch_ref='global_wallpaper_carousel'
              |    )
              |    and gaid is not null
              |""".stripMargin)
        val appChooser = sparkSession.sql(
            """
              |select
              |    gaid,
              |    package_name
              |from
              |    ods_ias_client_track
              |where
              |    first_page_type in ('minicard', 'innerpage')
              |    and (
              |        (
              |            launch_pkg='com.miui.home'
              |            and launch_ref='recent'
              |        )
              |        or (
              |            launch_pkg='com.miui.home'
              |            and launch_ref='searchShotcut'
              |        )
              |        or (
              |            launch_pkg in (
              |                'com.miui.msa.global',
              |                'com.miui.securitycenter',
              |                'com.android.systemui',
              |                'com.mi.globalbrowser',
              |                'com.android.browser',
              |                'com.mi.android.globalminusscreen',
              |                'com.miui.cleanmaster',
              |                'com.miui.cleaner',
              |                'com.xm.discover',
              |                'com.miui.android.fashiongallery',
              |                'com.miui.gallery',
              |                'com.miui.videoplayer',
              |                'com.mi.android.globalpersonalassistant',
              |                'com.android.thememanager',
              |                'com.xm.midrop',
              |                'com.android.calendar',
              |                'com.mi.android.globalFileexplorer',
              |                'com.miui.bugreport',
              |                'com.miui.calculator',
              |                'com.xm.bluetooth',
              |                'com.xm.calendar',
              |                'com.xm.hm.health',
              |                'com.android.providers.downloads.ui',
              |                'com.miui.player',
              |                'cn.wps.xm.abroad.lite'
              |            )
              |        )
              |    )
              |""".stripMargin
        )
        val result = miuiAd.rdd.map(row => (row.getString(0), row.getString(1)))
            .union(appChooser.rdd.map(row => (row.getString(0), row.getString(1))))
            .groupByKey()
            .map(row => (row._1, row._2.toSet))
            .toDF("id", "passiveAction")
        result.show(false)
        result.printSchema()
        result
    }

}
