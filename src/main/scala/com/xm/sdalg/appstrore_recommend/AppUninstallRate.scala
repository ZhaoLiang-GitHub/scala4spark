package com.xm.sdalg.appstrore_recommend

import com.xm.sdalg.appstrore_recommend.Utils.{getAppSetBc, getLatestAppSetBC}
import com.xm.sdalg.commons.SparkContextUtils.createSparkSession
import com.xm.sdalg.commons.TimeUtils.getFrontDay
import org.apache.spark.sql.SparkSession

/**
 * app 卸载率计算
 * 统计过去30天的卸载率，覆盖写到路径中，在召回时将CD级高卸载率的app过滤掉不在召回
 */
object AppUninstallRate {
    def main(args: Array[String]): Unit = {
        val Array(locale: String, endDate: String) = args
        val sparkSession: SparkSession = createSparkSession(this.getClass.getSimpleName)
        val res = getUninstallRate(sparkSession, locale)
        res.show(false)
        res.printSchema()
        res.write.mode("overwrite").parquet("/user/h_data_platform/platform/appstore/dwd_appstore_recommend_app_uninstall_rate/ID")
    }

    def getUninstallRate(sparkSession: SparkSession, locale: String) = {
        sparkSession.sql(
            s"""
               |WITH
               |    install AS (
               |        SELECT
               |            imeimd5 AS gaid,
               |            package_name AS pkg,
               |            server_time AS event_time,
               |            date
               |        FROM
               |            hive_alsgprc_hadoop.dw.dwd_app_stat_event_di
               |        WHERE
               |            date BETWEEN ${getFrontDay(30)} AND ${getFrontDay(2)}
               |            AND user_id=0
               |            AND app_type=0
               |            AND (
               |                imeimd5!='00000000-0000-0000-0000-000000000000'
               |                AND imeimd5 IS NOT NULL
               |            )
               |            AND install_source IS NOT NULL
               |            AND event_type='install'
               |            AND package_name IN (
               |                SELECT DISTINCT
               |                    packagename
               |                from
               |                    iceberg_alsgprc_hadoop.appstore.dwd_ias_recommend_app_di
               |                where
               |                    date='${getFrontDay(2)}'
               |                    and locale='ID'
               |                    and level in ('C','D')
               |            )
               |            AND package_name IS NOT NULL
               |            AND install_source='com.xm.mipicks'
               |            and region='ID'
               |        GROUP BY
               |            imeimd5,
               |            package_name,
               |            server_time,
               |            date
               |    ),
               |    uninstall AS (
               |        SELECT
               |            imeimd5 AS gaid,
               |            package_name AS pkg,
               |            server_time AS event_time,
               |            date
               |        FROM
               |            hive_alsgprc_hadoop.dw.dwd_app_stat_event_di
               |        WHERE
               |            date BETWEEN ${getFrontDay(30)} AND ${getFrontDay(2)}
               |            AND user_id=0
               |            AND (
               |                imeimd5!='00000000-0000-0000-0000-000000000000'
               |                AND imeimd5 IS NOT NULL
               |            )
               |            AND event_type='uninstall'
               |            AND package_name IN (
               |                SELECT DISTINCT
               |                    packagename
               |                from
               |                    iceberg_alsgprc_hadoop.appstore.dwd_ias_recommend_app_di
               |                where
               |                    date='${getFrontDay(2)}'
               |                    and locale='ID'
               |                    and level in ('C','D')
               |            )
               |            AND package_name IS NOT NULL
               |            and region='ID'
               |        GROUP BY
               |            imeimd5,
               |            package_name,
               |            server_time,
               |            date
               |    ),
               |    res AS (
               |        SELECT
               |            install.pkg,
               |            install.date,
               |            count(DISTINCT install.gaid) AS install_uv,
               |            count(
               |                DISTINCT if (
               |                    install.event_time<uninstall.event_time
               |                    AND uninstall.event_time-install.event_time<=3600000,
               |                    install.gaid,
               |                    NULL
               |                )
               |            ) AS hour_uninstall_uv,
               |            count(
               |                DISTINCT if (
               |                    install.event_time<uninstall.event_time
               |                    AND uninstall.event_time-install.event_time<=3600000*24,
               |                    install.gaid,
               |                    NULL
               |                )
               |            ) AS day_uninstall_uv
               |        FROM
               |            install
               |            LEFT JOIN uninstall ON install.gaid=uninstall.gaid
               |            AND install.pkg=uninstall.pkg
               |            AND install.date=uninstall.date
               |        GROUP BY
               |            install.pkg,
               |            install.date
               |        ORDER BY
               |            install.pkg,
               |            install.date
               |    )
               |SELECT
               |    res.pkg,
               |    sum(install_uv) AS install_uv_filter,
               |    sum(hour_uninstall_uv) AS hour_uninstall_uv_filter,
               |    sum(day_uninstall_uv) AS day_uninstall_uv_filter,
               |    round(sum(hour_uninstall_uv)/sum(install_uv), 4) hour_uninstall_rate_filter,
               |    round(sum(day_uninstall_uv)/sum(install_uv), 4) day_uninstall_rate_filter
               |FROM
               |    res
               |WHERE
               |    hour_uninstall_uv>0
               |    OR day_uninstall_uv>0
               |GROUP BY
               |    res.pkg
               |""".stripMargin)
            .join(getLatestAppSetBC(sparkSession, locale), Seq("pkg"))
    }

}
