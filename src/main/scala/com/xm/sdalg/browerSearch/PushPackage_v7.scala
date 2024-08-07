package com.xm.sdalg.browerSearch

import com.xm.sdalg.commons.SparkContextUtils.createSparkSession
import com.xm.sdalg.commons.TimeUtils.{getCurrentDateStr, getFrontDay}
import org.apache.spark.sql.SparkSession

/**
 * liping18-20240511
 * 圈选人包：月点击>0，30天内未搜索，随机选择[8,22]内时段半点下发
 */
object PushPackage_v7 {
    def main(args: Array[String]): Unit = {
        val sparkSession: SparkSession = createSparkSession(this.getClass.getSimpleName)
        val df = sparkSession.sql(
              s"""
                 |--对目标人群随机选择下发时段
                 |with
                 |--点击用户筛选
                 |push_click_user_inmonth as(
                 |    SELECT
                 |        distinct(userid) as userid
                 |    from
                 |        hive_zjyprc_hadoop.browser.dwd_push_user_level_browser_df
                 |    WHERE
                 |        date=${getFrontDay(1)}
                 |        and appname='browser'
                 |        and push_click_pv_month>0
                 |)
                 |--搜索用户筛选
                 |,search_user_inmonth as(
                 |    SELECT
                 |        -- *
                 |        distinct(distinct_id) as userid
                 |    from iceberg_zjyprc_hadoop.dw.dwd_ot_event_di_31000000442
                 |    WHERE
                 |        date <=${getFrontDay(1)} and date >=${getFrontDay(30)}
                 |        AND event_name = 'search'
                 |)
                 |--目标用户筛选
                 |,object_user as(
                 |    select
                 |        -- count(u1.userid )
                 |        distinct(u1.userid) as userid
                 |    from push_click_user_inmonth u1
                 |    left anti join search_user_inmonth u2
                 |    on u1.userid=u2.userid
                 |    )
                 | SELECT
                 |    userid,
                 |    FLOOR(RAND() * 15) + 8 as hour_of_day
                 |FROM
                 |    object_user
                 |
                 |""".stripMargin)
        for (hour_of_day <- 8 to 22) {
            // 筛选出当前小时的行，并选择"user_id"列
            val filteredRows = df.where(s"hour_of_day = '${hour_of_day}'").selectExpr("userid")
            // 计算截止到当前小时的累计PV和UV
            val cumulativePV = df.where(s"hour_of_day <= '${hour_of_day}' and hour_of_day >= 8").selectExpr("userid").count()
            val cumulativeUV = df.where(s"hour_of_day <= '${hour_of_day}' and hour_of_day >= 8").selectExpr("userid").distinct().count()

            // 打印截止到当前小时的累计PV和UV
            println(s"截止到 $hour_of_day 点的累计PV: $cumulativePV")
            println(s"截止到 $hour_of_day 点的累计UV: $cumulativeUV")

            filteredRows.repartition(2).write.mode("overwrite")
              .csv(s"/user/h_data_platform/platform/browser/dwd_browser_push_user_package/packagename=browser-search-v7-${hour_of_day}30/date=${getCurrentDateStr()}")
            sparkSession.sql(
                s"""
                   |alter table hive_zjyprc_hadoop.browser.dwd_browser_push_user_package add if not exists partition(packagename='browser-search-v7-${hour_of_day}30',date=${getCurrentDateStr()})
                   |""".stripMargin)
        }
    }

}
