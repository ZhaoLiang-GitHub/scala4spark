package com.xm.sdalg.browerSearch

import com.xm.sdalg.commons.SparkContextUtils.createSparkSession
import com.xm.sdalg.commons.TimeUtils.{getCurrentDateStr, getFrontDay}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object PushPackage_v2 {
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
         |                DISTINCT distinct_id,
         |                explode(collect_list(from_unixtime(e_ts / 1000,'yyyy-MM-dd HH:mm:ss'))) AS search_time
         |            FROM
         |                iceberg_zjyprc_hadoop.dw.dwd_ot_event_di_31000000442
         |            WHERE
         |                date >= ${getFrontDay(7)} AND date <= ${getFrontDay(1)}
         |                AND event_name = 'search'
         |            GROUP BY
         |                distinct_id
         |        ) AS subquery
         |    GROUP BY
         |        distinct_id, hour_of_day
         |),
         |
         |
         |push_expose_user_inweek AS (
         |    SELECT
         |        distinct userid
         |    FROM
         |        hive_zjyprc_hadoop.browser.dwd_push_user_level_browser_df
         |    WHERE
         |        date = ${getFrontDay(1)}
         |        AND appname = 'browser'
         |        AND push_click_pv_week > 1
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
         |
         |SELECT
         |    u1.hour_of_day,
         |    u1.distinct_id as user_id
         |FROM
         |    user_hours u1
         |JOIN
         |    push_expose_user_inweek u2 ON u1.distinct_id = u2.userid
         |LEFT JOIN
         |    exclude_users u3 ON u1.distinct_id = u3.distinct_id
         |WHERE
         |    u3.distinct_id IS NULL
         |    AND u1.hour_of_day BETWEEN 9 AND 22;
         |
         |""".stripMargin)
      .cache()

    for (hour_of_day <- 9 to 22) {
      // 筛选出当前小时的行，并选择"user_id"列
      val filteredRows = df.where(s"hour_of_day = '${hour_of_day}'").selectExpr("user_id")
        .sample(0.1)
      // 计算截止到当前小时的累计PV和UV
      val cumulativePV = df.where(s"hour_of_day <= '${hour_of_day}'").selectExpr("user_id").count()
      val cumulativeUV = df.where(s"hour_of_day <= '${hour_of_day}'").selectExpr("user_id").distinct().count()

      // 打印截止到当前小时的累计PV和UV
      println(s"截止到 $hour_of_day 点的累计PV: $cumulativePV")
      println(s"截止到 $hour_of_day 点的累计UV: $cumulativeUV")


      filteredRows.repartition(2).write.mode("overwrite").csv(s"/user/h_data_platform/platform/browser/dwd_browser_push_user_package/packagename=browser-search-v2-${hour_of_day}30/date=${getCurrentDateStr()}")
    }
  }
}
