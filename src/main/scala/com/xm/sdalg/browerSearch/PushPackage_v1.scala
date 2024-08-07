package com.xm.sdalg.browerSearch

import com.xm.sdalg.commons.SparkContextUtils.createSparkSession
import com.xm.sdalg.commons.TimeUtils.{getCurrentDateStr, getFrontDay}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * 【用户搜索时段分组】
 * sql筛选条件：6天搜索 & 1天未搜索 & 一月内push点击 > 1
 */
object PushPackage_v1 {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = createSparkSession(this.getClass.getSimpleName)
    val df = sparkSession.sql(
      s"""
         |WITH user_hours AS (
         |    SELECT
         |        distinct_id,
         |        hour(search_time) AS hour_of_day,
         |        COUNT(*) AS usage_count
         |    FROM
         |        (
         |            SELECT
         |                distinct_id,
         |                explode(collect_list(from_unixtime(e_ts / 1000,'yyyy-MM-dd HH:mm:ss'))) AS search_time
         |            FROM
         |                iceberg_zjyprc_hadoop.dw.dwd_ot_event_di_31000000442
         |            WHERE
         |                date >= ${getFrontDay(7)} AND date <= ${getFrontDay(2)}
         |                AND event_name = 'search'
         |            GROUP BY
         |                distinct_id
         |        ) AS subquery
         |    GROUP BY
         |        distinct_id, hour_of_day
         |),
         |
         |users_on_date_1 AS (
         |    SELECT DISTINCT distinct_id
         |    FROM iceberg_zjyprc_hadoop.dw.dwd_ot_event_di_31000000442
         |    WHERE date = ${getFrontDay(1)}
         |      AND event_name = 'search'
         |),
         |
         |push_expose_user_inmonth AS(
         |    SELECT
         |        distinct userid
         |    FROM
         |        hive_zjyprc_hadoop.browser.dwd_push_user_level_browser_df
         |    WHERE
         |        date = ${getFrontDay(1)}
         |        AND appname='browser'
         |        AND push_click_pv_month > 1
         |),
         |
         |exclude_users AS (
         |    SELECT DISTINCT userid AS distinct_id
         |    FROM hive_zjyprc_hadoop.browser.dwd_browser_push_user_package
         |    WHERE
         |        date = ${getFrontDay(1)}
         |        AND packagename = 'browser-search-nosearchuser'
         |)
         |
         |SELECT
         |    u1.hour_of_day,
         |    u1.distinct_id as user_id
         |FROM
         |    user_hours u1
         |LEFT JOIN
         |    users_on_date_1 u2 ON u1.distinct_id = u2.distinct_id
         |JOIN
         |    push_expose_user_inmonth u3 ON u1.distinct_id = u3.userid
         |LEFT JOIN
         |    exclude_users u4 ON u1.distinct_id = u4.distinct_id
         |WHERE
         |    u2.distinct_id IS NULL
         |    AND u4.distinct_id IS NULL
         |    AND u1.hour_of_day BETWEEN 8 AND 23;
         |
         |""".stripMargin)

    for (hour_of_day <- 8 to 23) {
      // 过滤出第一列值为hour_of_day的行，并获取第二列数据
      val filteredRows = df.where(s"hour_of_day = '${hour_of_day}'").selectExpr("user_id")
      filteredRows.repartition(2).write.mode("overwrite").csv(s"/user/h_data_platform/platform/browser/dwd_browser_push_user_package/packagename=browser-search-${hour_of_day}30/date=${getCurrentDateStr()}")
    }

  }
}
