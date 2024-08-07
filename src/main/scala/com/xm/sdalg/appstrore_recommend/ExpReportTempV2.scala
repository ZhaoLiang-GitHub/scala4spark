package com.xm.sdalg.appstrore_recommend

import com.xm.sdalg.commons.MysqlUtils._
import com.xm.sdalg.commons.SparkContextUtils._
import com.xm.sdalg.commons.TimeUtils._
import org.apache.spark.sql._

object ExpReportTempV2 {

    def main(args: Array[String]): Unit = {
        val Array(locale: String, date: String) = args.slice(0, 2)
        val sparkSession: SparkSession = createSparkSession(this.getClass.getSimpleName)
        val org = sparkSession.sql(
            s"""
               |WITH
               |    app_level as (
               |        SELECT
               |            packagename,
               |            level
               |        from
               |            iceberg_alsgprc_hadoop.appstore.dwd_ias_recommend_app_di
               |        WHERE
               |            locale='ID'
               |            and date=${getFrontDay(date, 1)}
               |    ),
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
               |            hour(from_unixtime(time/1000, 'yyyy-MM-dd HH:mm:ss')) AS download_hour,
               |            app_level.level AS level
               |        FROM
               |            hive_alsgprc_hadoop.dw.dwd_appstore_client_track a
               |            join app_level on a.package_name=app_level.packagename
               |        WHERE
               |            date BETWEEN ${date} AND ${date}
               |            AND lo='${locale}'
               |            AND market_v>=4002000
               |            AND ext_rec IS NOT NULL
               |            AND gaid IS NOT NULL
               |    ),
               |    uninstall AS (
               |        SELECT
               |            imeimd5 AS gaid,
               |            package_name AS pkg,
               |            event_time,
               |            date
               |        FROM
               |            hive_alsgprc_hadoop.dw.dwd_app_stat_event_di
               |        WHERE
               |            date BETWEEN ${date} AND ${date}
               |            AND user_id=0
               |            AND (
               |                imeimd5!='00000000-0000-0000-0000-000000000000'
               |                AND imeimd5 IS NOT NULL
               |            )
               |            AND event_type='uninstall'
               |            AND package_name NOT IN (
               |                'com.xm.smarthome',
               |                'com.duokan.phone.remotecontroller',
               |                'com.miui.player',
               |                'com.mi.global.shop',
               |                'com.miui.videoplayer',
               |                'com.mi.globalbrowser',
               |                'com.mipay.wallet.in',
               |                'com.xm.midrop',
               |                'com.xm.hm.health',
               |                'com.mi.launcher',
               |                'com.mi.global.bbs',
               |                'com.mi.global.mimover',
               |                'com.xm.router',
               |                'com.android.fileexplorer',
               |                'com.mi.android.globalFileexplorer',
               |                'com.miui.home',
               |                'com.android.providers.downloads.ui',
               |                'com.android.thememanager',
               |                'com.miui.securitycenter',
               |                'com.miui.cleanmaster',
               |                'com.android.browser',
               |                'com.miui.global.packageinstaller',
               |                'com.miui.msa.global',
               |                'com.android.calendar',
               |                'com.miui.weather2',
               |                'com.mi.android.globalminusscreen',
               |                'com.miui.cleaner',
               |                'com.mi.globalminusscreen',
               |                'com.android.thememanager',
               |                'com.miui.android.fashiongallery',
               |                'com.android.mms',
               |                'com.miui.cleanmaster',
               |                'com.mi.android.globalpersonalassistant',
               |                'com.miui.calculator',
               |                'com.xm.mipicks',
               |                'com.miui.mishare.connectivity',
               |                'com.miui.hybrid',
               |                'com.android.camera',
               |                'com.miui.miservice',
               |                'com.miui.screenrecorder',
               |                'com.android.deskclock',
               |                'com.miui.fm',
               |                'com.android.soundrecorder',
               |                'com.xm.scanner',
               |                'com.miui.compass',
               |                'com.miui.notes',
               |                'com.miui.gallery',
               |                'com.miui.cloudservice',
               |                'com.xm.account',
               |                'com.mi.android.globallauncher',
               |                'com.google.android.googlequicksearchbox',
               |                'com.google.android.inputmethod.latin',
               |                'com.android.contacts',
               |                'com.android.mms',
               |                'com.android.camera',
               |                'com.android.calendar',
               |                'com.android.vending'
               |            )
               |            AND package_name IS NOT NULL
               |            AND region='${locale}'
               |    ),
               |    uninstall_data AS (
               |        SELECT
               |            gaid,
               |            pkg,
               |            date,
               |            uninstall_hour,
               |            max(event_time) AS event_time
               |        FROM
               |            (
               |                SELECT
               |                    gaid,
               |                    pkg,
               |                    event_time,
               |                    from_unixtime(event_time/1000, 'yyyyMMdd') AS date,
               |                    hour(
               |                        from_unixtime(event_time/1000, 'yyyy-MM-dd HH:mm:ss')
               |                    ) AS uninstall_hour
               |                FROM
               |                    uninstall
               |            )
               |        GROUP BY
               |            gaid,
               |            pkg,
               |            date,
               |            uninstall_hour
               |    ),
               |    track AS (
               |        SELECT
               |            raw_track.*,
               |            uninstall_hour,
               |            event_time
               |        FROM
               |            raw_track
               |            left JOIN uninstall_data ON raw_track.package_name=uninstall_data.pkg
               |            AND raw_track.gaid=uninstall_data.gaid
               |            AND raw_track.date=uninstall_data.date
               |        WHERE
               |            raw_track.exp_date=raw_track.date
               |    ),
               |    exposure_data AS (
               |        SELECT
               |            date,
               |            model_id,
               |            level,
               |            count(
               |                if (
               |                    cur_page_type IN (
               |                        'recommend',
               |                        'client_homepage',
               |                        'native_home_feature'
               |                    )
               |                    AND action_type='item_exposure',
               |                    gaid,
               |                    NULL
               |                )
               |            ) AS exposure_pv,
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
               |            ) AS exposure_uv,
               |            count(
               |                DISTINCT if (
               |                    cur_page_type IN (
               |                        'recommend',
               |                        'client_homepage',
               |                        'native_home_feature'
               |                    )
               |                    AND action_type='item_exposure',
               |                    package_name,
               |                    NULL
               |                )
               |            ) AS exposure_app,
               |            count(
               |                if (
               |                    cur_page_type IN (
               |                        'recommend',
               |                        'client_homepage',
               |                        'native_home_feature'
               |                    )
               |                    AND action_type='item_click',
               |                    gaid,
               |                    NULL
               |                )
               |            ) AS click_pv,
               |            count(
               |                DISTINCT if (
               |                    cur_page_type IN (
               |                        'recommend',
               |                        'client_homepage',
               |                        'native_home_feature'
               |                    )
               |                    AND action_type='item_click',
               |                    gaid,
               |                    NULL
               |                )
               |            ) AS click_uv,
               |            count(
               |                DISTINCT if (
               |                    cur_page_type IN (
               |                        'recommend',
               |                        'client_homepage',
               |                        'native_home_feature'
               |                    )
               |                    AND action_type='item_click',
               |                    package_name,
               |                    NULL
               |                )
               |            ) AS click_app,
               |             count(
               |                if (
               |                    cur_page_type IN (
               |                        'recommend',
               |                        'client_homepage',
               |                        'native_home_feature'
               |                    )
               |                    AND action_type='item_exposure',
               |                    package_name,
               |                    NULL
               |                )
               |            ) AS exposure_app_pv
               |        FROM
               |            (
               |                SELECT
               |                    gaid,
               |                    date,
               |                    cur_page_type,
               |                    action_type,
               |                    package_name,
               |                    explode (model_id) as model_id,
               |                    level
               |                FROM
               |                    track
               |            )
               |        GROUP BY
               |            date,
               |            model_id,
               |            level
               |    ),
               |    download_data AS (
               |        SELECT
               |            date,
               |            model_id,
               |            level,
               |            count(gaid) AS download_pv,
               |            count(DISTINCT gaid) AS download_uv,
               |            count(DISTINCT concat(gaid, package_name)) AS download_puv,
               |            count(DISTINCT package_name) AS download_app,
               |            count(
               |                if (
               |                    uninstall_hour IS NULL
               |                    OR event_time-download_time>3600000,
               |                    gaid,
               |                    NULL
               |                )
               |            ) AS eff_download_pv,
               |            count(
               |                DISTINCT if (
               |                    uninstall_hour IS NULL
               |                    OR event_time-download_time>3600000,
               |                    gaid,
               |                    NULL
               |                )
               |            ) AS eff_download_uv,
               |            count(
               |                DISTINCT if (
               |                    uninstall_hour IS NULL
               |                    OR event_time-download_time>3600000,
               |                    package_name,
               |                    NULL
               |                )
               |            ) AS eff_download_app,
               |            count(
               |                if (
               |                    uninstall_hour IS NULL
               |                    OR event_time-download_time>3600000,
               |                    concat(gaid, package_name),
               |                    NULL
               |                )
               |            ) AS eff_download_puv,
               |            count(DISTINCT if (event_time>download_time, gaid, NULL)) AS uninstall_uv,
               |            count(
               |                DISTINCT if (event_time>download_time, package_name, NULL)
               |            ) AS uninstall_app,
               |            count(
               |                if (
               |                    event_time>download_time,
               |                    concat(gaid, package_name),
               |                    NULL
               |                )
               |            ) AS uninstall_puv,
               |            count(
               |                if (
               |                    event_time>download_time,
               |                    concat(gaid, package_name),
               |                    NULL
               |                )
               |            ) AS uninstall_puv_day,
               |            count(
               |                if (
               |                    event_time>download_time
               |                    and event_time-download_time<3600000,
               |                    concat(gaid, package_name),
               |                    NULL
               |                )
               |            ) as uninstall_puv_hour
               |        FROM
               |            (
               |                SELECT
               |                    gaid,
               |                    date,
               |                    explode (model_id) as model_id,
               |                    package_name,
               |                    event_time,
               |                    download_time,
               |                    download_hour,
               |                    uninstall_hour,
               |                    level
               |                FROM
               |                    track
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
               |                UNION ALL
               |                SELECT
               |                    gaid,
               |                    date,
               |                    explode (model_id) as model_id,
               |                    package_name,
               |                    event_time,
               |                    download_time,
               |                    download_hour,
               |                    uninstall_hour,
               |                    level
               |                FROM
               |                    track
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
               |            )
               |        GROUP BY
               |            date,
               |            model_id,
               |            level
               |    ),
               |    install_data AS (
               |        SELECT
               |            date,
               |            model_id,
               |            level,
               |            count(gaid) AS install_pv,
               |            count(DISTINCT gaid) AS install_uv,
               |            count(DISTINCT concat(gaid, package_name)) AS install_puv,
               |            count(DISTINCT package_name) AS install_app,
               |            count(
               |                if (
               |                    uninstall_hour IS NULL
               |                    OR event_time-download_time>3600000,
               |                    gaid,
               |                    NULL
               |                )
               |            ) AS eff_install_pv,
               |            count(
               |                DISTINCT if (
               |                    uninstall_hour IS NULL
               |                    OR event_time-download_time>3600000,
               |                    gaid,
               |                    NULL
               |                )
               |            ) AS eff_install_uv,
               |            count(
               |                DISTINCT if (
               |                    uninstall_hour IS NULL
               |                    OR event_time-download_time>3600000,
               |                    package_name,
               |                    NULL
               |                )
               |            ) AS eff_install_app,
               |            count(
               |                if (
               |                    uninstall_hour IS NULL
               |                    OR event_time-download_time>3600000,
               |                    concat(gaid, package_name),
               |                    NULL
               |                )
               |            ) AS eff_install_puv
               |        FROM
               |            (
               |                SELECT
               |                    gaid,
               |                    date,
               |                    explode (model_id) as model_id,
               |                    package_name,
               |                    event_time,
               |                    download_time,
               |                    download_hour,
               |                    uninstall_hour,
               |                    level
               |                FROM
               |                    track
               |                WHERE
               |                    cur_page_type in (
               |                        'recommend',
               |                        'client_homepage',
               |                        'native_home_feature'
               |                    )
               |                    AND cur_card_type='recApps'
               |                    AND action_type='download_install'
               |                    AND install_status='install_success'
               |                    AND install_type in ('manual_install', 'auto_install')
               |                UNION ALL
               |                SELECT
               |                    gaid,
               |                    date,
               |                    explode (model_id) as model_id,
               |                    package_name,
               |                    event_time,
               |                    download_time,
               |                    download_hour,
               |                    uninstall_hour,
               |                    level
               |                FROM
               |                    track
               |                WHERE
               |                    cur_page_type like '%detail%'
               |                    AND pre_page_type in (
               |                        'recommend',
               |                        'client_homepage',
               |                        'native_home_feature'
               |                    )
               |                    AND pre_card_type='recApps'
               |                    AND action_type='download_install'
               |                    AND install_status='install_success'
               |                    AND install_type in ('manual_install', 'auto_install')
               |            )
               |        GROUP BY
               |            date,
               |            model_id,
               |            level
               |    ),
               |    active_data AS (
               |        SELECT
               |            date,
               |            model_id,
               |            level,
               |            count(gaid) AS active_pv,
               |            count(DISTINCT gaid) AS active_uv,
               |            count(DISTINCT concat(gaid, package_name)) AS active_puv,
               |            count(DISTINCT package_name) AS active_app,
               |            count(
               |                if (
               |                    uninstall_hour IS NULL
               |                    OR event_time-download_time>3600000,
               |                    gaid,
               |                    NULL
               |                )
               |            ) AS eff_active_pv,
               |            count(
               |                DISTINCT if (
               |                    uninstall_hour IS NULL
               |                    OR event_time-download_time>3600000,
               |                    gaid,
               |                    NULL
               |                )
               |            ) AS eff_active_uv,
               |            count(
               |                DISTINCT if (
               |                    uninstall_hour IS NULL
               |                    OR event_time-download_time>3600000,
               |                    package_name,
               |                    NULL
               |                )
               |            ) AS eff_active_app,
               |            count(
               |                if (
               |                    uninstall_hour IS NULL
               |                    OR event_time-download_time>3600000,
               |                    concat(gaid, package_name),
               |                    NULL
               |                )
               |            ) AS eff_active_puv
               |        FROM
               |            (
               |                SELECT
               |                    gaid,
               |                    date,
               |                    explode (model_id) as model_id,
               |                    package_name,
               |                    event_time,
               |                    download_time,
               |                    download_hour,
               |                    uninstall_hour,
               |                    level
               |                FROM
               |                    track
               |                WHERE
               |                    cur_page_type in (
               |                        'recommend',
               |                        'client_homepage',
               |                        'native_home_feature'
               |                    )
               |                    AND cur_card_type='recApps'
               |                    AND action_type='activate'
               |                UNION ALL
               |                SELECT
               |                    gaid,
               |                    date,
               |                    explode (model_id) as model_id,
               |                    package_name,
               |                    event_time,
               |                    download_time,
               |                    download_hour,
               |                    uninstall_hour,
               |                    level
               |                FROM
               |                    track
               |                WHERE
               |                    cur_page_type like '%detail%'
               |                    AND pre_page_type in (
               |                        'recommend',
               |                        'client_homepage',
               |                        'native_home_feature'
               |                    )
               |                    AND pre_card_type='recApps'
               |                    AND action_type='activate'
               |            )
               |        GROUP BY
               |            date,
               |            model_id,
               |            level
               |    )
               | SELECT
               |     exposure_data.model_id,
               |     exposure_data.level,
               |     exposure_data.date,
               |     avg(exposure_app_pv) as exposure_app_pv,  --曝光应用次数
               |     avg(download_puv) as download_puv,  --下载人包
               |     avg(install_puv) as install_puv,  --安装人包
               |     avg(uninstall_puv_day) as uninstall_puv_day,  --天卸载人包
               |     avg(uninstall_puv_hour) as uninstall_puv_hour,  --小时卸载人包
               |     round(avg(uninstall_puv_day)/avg(install_puv),4) as uninstall_day_rate, -- 天卸载人包 / 安装人包
               |     round(avg(uninstall_puv_hour)/avg(install_puv),4) as uninstall_hour_rate  -- 小时卸载人包 / 安装人包
               | FROM
               |     exposure_data
               |     JOIN download_data ON exposure_data.model_id=download_data.model_id
               |     AND exposure_data.date=download_data.date
               |     AND exposure_data.level=download_data.level
               |     JOIN install_data ON exposure_data.model_id=install_data.model_id
               |     AND exposure_data.date=install_data.date
               |     AND exposure_data.level=install_data.level
               |     JOIN active_data ON exposure_data.model_id=active_data.model_id
               |     AND exposure_data.date=active_data.date
               |     AND exposure_data.level=active_data.level
               | GROUP BY
               |     exposure_data.model_id,
               |     exposure_data.level,
               |     exposure_data.date
               |""".stripMargin)

        org.show(false)
        org.printSchema()

        val resultRdd = org
            .rdd
            .flatMap(row => {
                val eid = "ID_" + row.getAs[String]("model_id")
                val level = row.getAs[String]("level")
                val page = "首页"
                val stat_type = "ALL"

                val exposure_app_pv = row.getAs[Double]("exposure_app_pv")
                val download_puv = row.getAs[Double]("download_puv")
                val install_puv = row.getAs[Double]("install_puv")
                val uninstall_puv_day = row.getAs[Double]("uninstall_puv_day")
                val uninstall_puv_hour = row.getAs[Double]("uninstall_puv_hour")
                val uninstall_day_rate = row.getAs[Double]("uninstall_day_rate")
                val uninstall_hour_rate = row.getAs[Double]("uninstall_hour_rate")
                val currentDateAndTime = getCurrentTimestamp


                Array(
                    (date, eid, s"${page}_${stat_type}_${level}_exposure_app_pv", exposure_app_pv, currentDateAndTime),
                    (date, eid, s"${page}_${stat_type}_${level}_download_puv", download_puv, currentDateAndTime),
                    (date, eid, s"${page}_${stat_type}_${level}_install_puv", install_puv, currentDateAndTime),
                    (date, eid, s"${page}_${stat_type}_${level}_uninstall_puv_day", uninstall_puv_day, currentDateAndTime),
                    (date, eid, s"${page}_${stat_type}_${level}_uninstall_puv_hour", uninstall_puv_hour, currentDateAndTime),
                    (date, eid, s"${page}_${stat_type}_${level}_uninstall_day_rate", uninstall_day_rate, currentDateAndTime),
                    (date, eid, s"${page}_${stat_type}_${level}_uninstall_hour_rate", uninstall_hour_rate, currentDateAndTime)
                )
            })
            .filter(row => {
                val value = row._4
                !value.isNaN && !value.isInfinite
            })
            .map(row => s"${row._1}\t${row._2}\t${row._3}\t${row._4}\t${row._5}")

        writeToMysql(
            sparkSession,
            resultRdd.map(_.split("\t")).map(row => Array(row(0), row(1), row(2), row(3), row(4))),
            abtestColumn, c4_mig3_sdalg_stage00, mysqlPort, abtestDataBase, abtestAbdataTable, abtestUser, abtestPassword)
    }
}
