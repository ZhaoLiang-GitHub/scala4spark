package com.xm.sdalg.browerSearch

import com.xm.sdalg.commons.SparkContextUtils.createSparkSession
import com.xm.sdalg.commons.TimeUtils.getFrontDay
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author liping18 on 20240428
 *         实验数据写入实验平台
 *         通过下发时间区分expid，分别为整点下发和半点下发
 */
object ExpReportByPushTime {
    def main(args: Array[String]): Unit = {
        val sparkSession: SparkSession = createSparkSession(this.getClass.getSimpleName)
        val Array(pushDate: String) = args
        val frontPushDate = getFrontDay(pushDate, 1)
        val withExpid: DataFrame = sparkSession.sql(
            s"""
               |WITH
               |    temp as (
               |        select
               |            split(get_json_object(extra, '$$.param'), '\\\\|')[2] AS para_push_time
               |            ,*
               |        from hive_zjyprc_hadoop.browser.push_callback_log
               |        WHERE
               |            date =${pushDate} OR date=${frontPushDate}
               |    ),
               |    temp_with_expid AS(
               |        select
               |            substr(para_push_time, -14, 8) AS push_date,
               |            substr(para_push_time, -6, 2) AS push_hour,
               |            substr(para_push_time, -4, 2) AS push_min,
               |            substr(para_push_time, 1, length(para_push_time)-18) AS taskid,
               |            date as log_date,
               |            reachItems[0].stockId as itemid,
               |            reachItems[0].type as activetype,
               |            reachItems[0].ext as pushid,
               |            deviceid
               |        from temp
               |    )
               |select
               |    CASE
               |        WHEN push_min='00' THEN 'push_at_00min'
               |        WHEN push_min='30' THEN 'push_at_30min'
               |        WHEN push_min='15' THEN 'push_at_15min'
               |        WHEN push_min='45' THEN 'push_at_45min'
               |        ELSE NULL
               |    END AS expid,
               |    push_date,
               |    log_date,
               |    deviceid as userid,
               |    itemid,
               |    activetype,
               |    pushid
               |from
               |    temp_with_expid
               |where
               |    (push_date=${pushDate} OR push_date=${frontPushDate})
               |    and activetype in ('CLICK', 'EXPOSE')
               |    and deviceid is not null
               |    and deviceid!=''
               |    and itemid is not null
               |    and itemid!=''
               |    and(taskid='SH' or taskid='BSOF')
               |
               |""".stripMargin)
        //计算实验指标
        Utils.getAndWritExpIndex(sparkSession, withExpid)
    }}
