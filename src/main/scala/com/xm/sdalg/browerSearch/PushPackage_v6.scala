package com.xm.sdalg.browerSearch

import com.xm.sdalg.commons.SparkContextUtils.createSparkSession
import com.xm.sdalg.commons.TimeUtils.{getCurrentDateStr, getFrontDay}
import org.apache.spark.sql.SparkSession

object PushPackage_v6 {
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
           |                date >= ${getFrontDay(30)} AND date <= ${getFrontDay(1)}
           |                AND event_name = 'search'
           |            GROUP BY
           |                distinct_id
           |        ) AS subquery
           |    GROUP BY
           |        distinct_id, hour_of_day
           |),
           |
           |push_expose_user_inweek AS (
           |    SELECT
           |        distinct userid
           |    FROM
           |        hive_zjyprc_hadoop.browser.dwd_push_user_level_browser_df
           |    WHERE
           |        date = ${getFrontDay(1)}
           |        AND appname = 'browser'
           |        AND push_click_pv_week > 0
           |),
           |
           |push_expose_user_inmonth AS (
           |    SELECT
           |        distinct userid
           |    FROM
           |        hive_zjyprc_hadoop.browser.dwd_push_user_level_browser_df
           |    WHERE
           |        date = ${getFrontDay(1)}
           |        AND appname = 'browser'
           |        AND push_click_pv_month > 0
           |),
           |
           |push_user AS (
           |    SELECT
           |        distinct t2.userid
           |    FROM
           |        push_expose_user_inmonth t2
           |    left join
           |        push_expose_user_inweek t1 ON t1.userid = t2.userid
           |    where
           |        t1.userid is null
           |)
           |
           |
           |SELECT
           |    hour_of_day,
           |    distinct_id AS user_id
           |FROM
           |    user_hours u1
           |JOIN
           |    push_user u2 ON u1.distinct_id = u2.userid
           |
           |""".stripMargin)
      .cache()

    for (hour_of_day <- 8 to 22) {
      // 筛选出当前小时的行，并选择"user_id"列
      val filteredRows = df.where(s"hour_of_day = '${hour_of_day}'").selectExpr("user_id")

      // 计算截止到当前小时的累计PV和UV
      val cumulativePV = df.where(s"hour_of_day <= '${hour_of_day}' and hour_of_day >= 8").selectExpr("user_id").count()
      val cumulativeUV = df.where(s"hour_of_day <= '${hour_of_day}' and hour_of_day >= 8").selectExpr("user_id").distinct().count()

      // 打印截止到当前小时的累计PV和UV
      println(s"截止到 $hour_of_day 点的累计PV: $cumulativePV")
      println(s"截止到 $hour_of_day 点的累计UV: $cumulativeUV")


      filteredRows.repartition(2).write.mode("overwrite").csv(s"/user/h_data_platform/platform/browser/dwd_browser_push_user_package/packagename=browser-search-v6-${hour_of_day}30/date=${getCurrentDateStr()}")
      sparkSession.sql(
        s"""
           |alter table hive_zjyprc_hadoop.browser.dwd_browser_push_user_package add if not exists partition(packagename='browser-search-v6-${hour_of_day}30',date=${getCurrentDateStr()})
           |""".stripMargin)
    }

  }
}
