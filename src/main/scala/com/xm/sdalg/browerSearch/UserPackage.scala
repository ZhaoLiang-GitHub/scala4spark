package com.xm.sdalg.browerSearch

import com.xm.sdalg.commons.HDFSUtils.dfSaveInAlpha
import com.xm.sdalg.browerSearch.Utils._
import com.xm.sdalg.commons.SparkContextUtils._
import com.xm.sdalg.commons.TimeUtils.{getCurrentDateStr, getFrontDay}
import org.apache.spark.sql.SparkSession

/**
 * 圈选人群包：近一周push点击&近一月未搜索用户
 */
object UserPackage {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = createSparkSession(this.getClass.getSimpleName)
    val df = sparkSession.sql(
        s"""
           |
           |with push_expose_user_inmonth as(
           |    SELECT
           |        distinct userid
           |    FROM
           |        hive_zjyprc_hadoop.browser.dwd_push_user_level_browser_df
           |    WHERE
           |        date=${getFrontDay(1)}
           |        and appname='browser'
           |        and push_click_days_in1week>1
           |        and push_click_pv_week>1
           |),
           |
           |search_user_inmonth as(
           |    SELECT
           |        distinct distinct_id as id
           |    FROM
           |        iceberg_zjyprc_hadoop.miuisearch.dm_browser_search_sug_item_event_device_1d
           |    WHERE
           |        date >= ${getFrontDay(30)} and date <= ${getFrontDay(1)}
           |)
           |
           |select
           |   t1.userid
           |from push_expose_user_inmonth t1
           |left join search_user_inmonth t2
           |on t1.userid = t2.id
           |where t2.id IS NULL
           |
                """.stripMargin)
      .union(getWhiteUserDf(sparkSession))
    df.write.mode("overwrite").csv(s"/user/h_data_platform/platform/browser/dwd_browser_push_user_package/packagename=browser-search-nosearchuser/date=${getCurrentDateStr()}")
    sparkSession.sql(
      s"""
         |alter table hive_zjyprc_hadoop.browser.dwd_browser_push_user_package add if not exists partition(packagename='browser-search-nosearchuser',date=${getCurrentDateStr()})
         |""".stripMargin)


  }
}

