package com.xm.sdalg.browerSearch

import com.xm.sdalg.commons.SparkContextUtils.createSparkSession
import com.xm.sdalg.commons.TimeUtils.{getCurrentDateStr, getFrontDay}
import org.apache.spark.sql.SparkSession

/**
 * 人群包圈选：
 * 周push点击。近30天有search行为的用户按serch时间下发，否则随机下发。
 */
object PushPackage_v4 {
    def main(args: Array[String]): Unit = {
        val sparkSession: SparkSession = createSparkSession(this.getClass.getSimpleName)
        val df = sparkSession.sql(
              s"""
                 |WITH
                 |push_users AS (
                 |    SELECT
                 |        distinct userid
                 |    FROM
                 |        hive_zjyprc_hadoop.browser.dwd_push_user_level_browser_df
                 |    WHERE
                 |        date = ${getFrontDay(1)}
                 |        AND appname = 'browser'
                 |        AND push_click_pv_week > 0
                 |        ),
                 |search_users as(
                 |    select
                 |        distinct_id,
                 |        e_ts
                 |    from iceberg_zjyprc_hadoop.dw.dwd_ot_event_di_31000000442 t1
                 |    WHERE
                 |        date >= ${getFrontDay(30)} AND date <= ${getFrontDay(1)}
                 |        AND event_name = 'search'
                 |        ),
                 |user_hours AS (
                 |    SELECT DISTINCT
                 |        userid,
                 |        hour(from_unixtime(e_ts / 1000,'yyyy-MM-dd HH:mm:ss')) AS hour_of_day
                 |    FROM search_users as t1
                 |    right join push_users as t2 on t1.distinct_id=t2.userid
                 |),
                 |
                 |no_search_hour as (
                 |    SELECT
                 |        userid,
                 |        FLOOR(RAND() * 15) + 8 as hour_of_day
                 |    FROM
                 |        user_hours
                 |    WHERE
                 |        hour_of_day is null
                 |),
                 |search_hour as (
                 |    SELECT
                 |        userid,
                 |        hour_of_day
                 |    FROM
                 |        user_hours
                 |    WHERE
                 |        hour_of_day is not null
                 |),
                 |all_user_hours as (
                 |    SELECT
                 |        COALESCE(t1.userid, t2.userid) AS user_id,
                 |        COALESCE(t1.hour_of_day, t2.hour_of_day) AS hour_of_day
                 |    FROM
                 |        search_hour t1
                 |    FULL OUTER JOIN no_search_hour t2 ON t1.userid = t2.userid
                 |)
                 |select
                 |    *
                 |from all_user_hours
                 |
                 |""".stripMargin)
          .cache().sample(0.13)
        import sparkSession.implicits._
        var resultDF = Seq.empty[(Int, Long, Long)].toDF("hour_of_day", "cumulativePV", "cumulativeUV")
        for (hour_of_day <- 8 to 22) {
            // 筛选出当前小时的行，并选择"user_id"列
            val filteredRows = df.where(s"hour_of_day = '${hour_of_day}'").selectExpr("user_id")

            // 计算截止到当前小时的累计PV和UV
            val cumulativePV = df.where(s"hour_of_day <= '${hour_of_day}' and hour_of_day>=8").selectExpr("user_id").count()
            val cumulativeUV = df.where(s"hour_of_day <= '${hour_of_day}' and hour_of_day>=8").selectExpr("user_id").distinct().count()

            // 将当前小时的结果添加到resultDF
            val currentResult = Seq((hour_of_day, cumulativePV, cumulativeUV)).toDF("hour_of_day", "cumulativePV", "cumulativeUV")
            resultDF = resultDF.union(currentResult)


            filteredRows.repartition(2).write.mode("overwrite").csv(s"/user/h_data_platform/platform/browser/dwd_browser_push_user_package/packagename=browser-search-v4-${hour_of_day}30/date=${getCurrentDateStr()}")
            sparkSession.sql(
                s"""
                   |alter table hive_zjyprc_hadoop.browser.dwd_browser_push_user_package add if not exists partition(packagename='browser-search-v4-${hour_of_day}30',date=${getCurrentDateStr()})
                   |""".stripMargin)
        }
        resultDF.show()
    }
}
