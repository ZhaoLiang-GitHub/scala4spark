package com.xm.sdalg.appstrore_recommend.clip

import com.xm.sdalg.commons.ClusterUtils.locale2catalog
import com.xm.sdalg.commons.HDFSUtils._
import com.xm.sdalg.commons.SparkContextUtils.createSparkSession
import com.xm.sdalg.commons.TimeUtils._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 获得要进行处理的icon
 */
object NewIcon {
    def main(args: Array[String]): Unit = {
        val Array(locale: String, date: String) = args.slice(0, 2)
        val sparkSession: SparkSession = createSparkSession(this.getClass.getSimpleName)
        val res: DataFrame = getCandidate(sparkSession, locale)
        res.repartition(1).write.mode("overwrite").csv("/user/h_sns/zhaoliang6/clip/new_icon")
    }

    def getCandidate(sparkSession: SparkSession, locale: String) = {
        import sparkSession.implicits._
        val date = getLastHiveDate(sparkSession, s"${locale2catalog(locale, "iceberg")}.appstore.dwd_ias_recommend_app_di")
        val newDf = sparkSession.sql(
            s"""
               |select packagename as packagename1, REPLACE(icon, 'AppStore/', '')  as icon1
               |from ${locale2catalog(locale, "iceberg")}.appstore.dwd_ias_recommend_app_di
               |where date=${date}
               |-- and locale = '${locale}'
               |""".stripMargin)
        val oldDf = sparkSession.sql(
            s"""
               |select packagename as packagename2, REPLACE(icon, 'AppStore/', '')  as icon2
               |from ${locale2catalog(locale, "iceberg")}.appstore.dwd_ias_recommend_app_di
               |where date=${getFrontDay(date, 1)}
               |-- and locale = '${locale}'
               |""".stripMargin)
        val changeIcon = newDf
            .join(oldDf, newDf("packagename1") === oldDf("packagename2"))
            .where("icon1 != icon2")
            .selectExpr("icon1 as icon")
        val newIcon = newDf.selectExpr("packagename1 as packagename", "icon1")
            .join(oldDf.selectExpr("packagename2 as packagename", "icon2"), Seq("packagename"), "left_anti")
            .where("icon1 is not null")
            .selectExpr("icon1 as icon")

        val res = newIcon
            .union(changeIcon)
            .distinct()
            .toDF("icon_id")
        newIcon.union(changeIcon).show(false)
        println(
            s"""
               |读取的日期是 ${date}
               |新增的icon数是 ${newIcon.count()}
               |变更的icon数是 ${changeIcon.count()}
               |总数是 ${res.count()}
               |""".stripMargin)
        res
    }
}