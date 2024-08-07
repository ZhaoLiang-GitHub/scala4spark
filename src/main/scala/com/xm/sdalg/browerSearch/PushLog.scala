package com.xm.sdalg.browerSearch

import com.xm.sdalg.commons.SparkContextUtils.createSparkSession
import com.xm.sdalg.commons.TimeUtils.getFrontDay
import org.apache.spark.sql.SparkSession

object PushLog {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = createSparkSession(this.getClass.getSimpleName)
    val df = sparkSession.sql(
        s"""
           |WITH temp AS (
           |    SELECT
           |        split(get_json_object(extra, '$$.param'), '\\\\|') [2] AS para_push_time,
           |        *
           |    FROM
           |        hive_zjyprc_hadoop.browser.push_callback_log
           |    WHERE
           |        date = ${getFrontDay(1)}
           |),
           |
           |temp_select AS (
           |    SELECT
           |        deviceid as id,
           |        reachItems,
           |        date,
           |        substr(para_push_time, 1, length(para_push_time)-18) AS taskid
           |    FROM
           |        temp
           |),
           |
           |browser_log AS (
           |    SELECT
           |        id,
           |        date,
           |        from_unixtime(reachItems[0].reachtime / 1000,'yyyy-MM-dd HH:mm:ss') AS reachtime,
           |        reachItems[0].stockId AS itemid,
           |        reachItems[0].type AS type,
           |        taskid,
           |        hour(from_unixtime(reachItems[0].reachtime / 1000, 'yyyy-MM-dd HH:mm:ss')) AS reach_hour,
           |        minute(from_unixtime(reachItems[0].reachtime / 1000, 'yyyy-MM-dd HH:mm:ss')) AS reach_minute
           |    FROM
           |        temp_select
           |    WHERE
           |        reachItems[0].type IN ('CLICK', 'EXPOSE')
           |        AND id IS NOT NULL
           |        AND id!=''
           |        AND reachItems[0].stockId IS NOT NULL
           |        AND reachItems[0].stockId!=''
           |)
           |
           |SELECT
           |    date,
           |    id,
           |    type,
           |    reachtime,
           |    taskid,
           |    itemid,
           |    reach_hour,
           |    CASE
           |        WHEN reach_minute < 15 THEN 15
           |        WHEN reach_minute < 30 THEN 30
           |        WHEN reach_minute < 45 THEN 45
           |        ELSE 60
           |    END AS assigned_minute
           |from
           |    browser_log
           |
           |""".stripMargin)
      .cache()


    // 覆盖写入 Iceberg 表
    df.write
      .format("iceberg")
      .mode("append")
      .save("iceberg_zjyprc_hadoop.browser.dwd_browser_search_push_reachtime")
  }
}
