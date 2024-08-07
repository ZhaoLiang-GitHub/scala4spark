package com.xm.sdalg.appstrore_recommend

import com.xm.sdalg.appstrore_recommend.Utils._
import com.xm.sdalg.commons.ClusterUtils._
import com.xm.sdalg.commons.HDFSUtils._
import com.xm.sdalg.commons.SparkContextUtils.createSparkSession
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * icon 相似度计算
 * 从 appstore-recommend com.xm.get.apps.recommend.icon.AppIconLimit 迁移过来
 */
object AppIconLimit {
    def main(args: Array[String]): Unit = {
        val Array(locale, date, front7DayLevel, front1DayLevel) = args
        val sparkSession: SparkSession = createSparkSession(this.getClass.getSimpleName)
        import sparkSession.implicits._
        val allApp = getAppSetBc(sparkSession, locale, date, front7DayLevel, front1DayLevel)
        val result: DataFrame = sparkSession.read.text("/user/h_sns/mipicks_recommend/app_icon/cosSim/result.csv")
            .rdd
            .map(_.getString(0).split("\t"))
            .filter(_.length == 2)
            .map(row => {
                val app = row(0)
                val sim = row(1)
                    .split(",")
                    .filter(_.nonEmpty)
                    .map(_.split("&"))
                    .map(i => (i(0).toString, i(1).toDouble))
                    .filter(_._2 > 0.87)
                    .filter(i => allApp.value.contains(i._1))
                    .slice(0, 5) // 原始数据是降序
                    .map(_._1)
                    .mkString(",")
                (app, sim)
            })
            .toDF("packageName", "simIcon")
        dfSaveInAssignPath(sparkSession, s"/user/h_sns/mipicks_recommend/app_icon/final/result_${locale2locale(locale)}.csv", result, "csv")

    }
}
