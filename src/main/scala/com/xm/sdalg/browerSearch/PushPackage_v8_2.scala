package com.xm.sdalg.browerSearch

import com.xm.sdalg.commons.SparkContextUtils.createSparkSession
import com.xm.sdalg.commons.TimeUtils.{getCurrentDateStr, getFrontDay}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.util.Random

/**
 * liping18-2024051y
 * 圈选人包：月度push>=1&月点击天数>=1，只取搜索用户
 * 下发时段：在[8,22]内随机选择n个时段下发，n=月点击天数
 * 下发条数：每个时段下发一条
 */
object PushPackage_v8_2 {
    def main(args: Array[String]): Unit = {
        val sparkSession: SparkSession = createSparkSession(this.getClass.getSimpleName)
        val sqlout = sparkSession.sql(
            s"""
               |WITH
               |push_click_user_inmonth as(
               |    SELECT
               |        distinct userid,
               |        case
               |           when push_click_days_in1month>6 then 6
               |           else push_click_days_in1month
               |        end as push_num
               |    FROM
               |        hive_zjyprc_hadoop.browser.dwd_push_user_level_browser_df
               |    WHERE
               |        date = ${getFrontDay(1)}
               |        AND appname = 'browser'
               |        AND push_click_pv_month >=1
               |        AND push_click_days_in1month >=1
               | ),
               |search_user_inmonth AS (
               |    SELECT
               |        DISTINCT distinct_id
               |    FROM
               |        iceberg_zjyprc_hadoop.dw.dwd_ot_event_di_31000000442
               |    WHERE
               |        date >= ${getFrontDay(30)} AND date <= ${getFrontDay(1)}
               |        AND event_name = 'search'
               |)
               |SELECT
               |    distinct t2.userid,
               |    push_num
               |FROM
               |    push_click_user_inmonth t2
               |join
               |    search_user_inmonth t1 ON t1.distinct_id = t2.userid
               |""".stripMargin)
        import sparkSession.implicits._
        val push_hours = List(10, 11, 13, 15, 18, 20)
        val df: DataFrame = sqlout.rdd
            .flatMap(r => {
                val userid = r.getAs[String]("userid")
                val push_num = r.getAs[Int]("push_num")
                val temp = Random.shuffle(push_hours).take(push_num)
                temp.map(l => (userid, l))
            }).toDF("userid", "hour_of_day")

        for (hour_of_day <- push_hours) {
            // 筛选出当前小时的行，并选择"user_id"列
            val filteredRows = df.where(s"hour_of_day = '${hour_of_day}'").selectExpr("userid")
            // 计算截止到当前小时的累计PV和UV
            val cumulativePV = df.where(s"hour_of_day <= '${hour_of_day}' and hour_of_day >= 8").selectExpr("userid").count()
            val cumulativeUV = df.where(s"hour_of_day <= '${hour_of_day}' and hour_of_day >= 8").selectExpr("userid").distinct().count()

            // 打印截止到当前小时的累计PV和UV
            println(s"截止到 $hour_of_day 点的累计PV: $cumulativePV")
            println(s"截止到 $hour_of_day 点的累计UV: $cumulativeUV")

            filteredRows.repartition(2).write.mode("overwrite")
                .csv(s"/user/h_data_platform/platform/browser/dwd_browser_push_user_package/packagename=browser-search-v8_2-${hour_of_day}30/date=${getCurrentDateStr()}")
            sparkSession.sql(
                s"""
                   |alter table hive_zjyprc_hadoop.browser.dwd_browser_push_user_package add if not exists partition(packagename='browser-search-v8_2-${hour_of_day}30',date=${getCurrentDateStr()})
                   |""".stripMargin)
        }
    }

}
