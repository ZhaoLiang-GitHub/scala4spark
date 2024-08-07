package com.xm.sdalg.appstrore_recommend

import com.xm.sdalg.appstrore_recommend.Utils._
import com.xm.sdalg.commons.HDFSUtils._
import com.xm.sdalg.commons.SparkContextUtils.createSparkSession
import com.xm.sdalg.commons.TimeUtils.getFrontDay
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.{avg, collect_set, sum}
import org.apache.spark.sql._
import com.xm.sdalg.commons.ClusterUtils._
import com.xm.sdalg.commons.ClusterUtils._

object UserBehaviorInPlat {
    def main(args: Array[String]): Unit = {
        val Array(locale: String, endDate: String) = args
        val sparkSession: SparkSession = createSparkSession(this.getClass.getSimpleName)
        val userBehaviorInPlatform = getUserBehaviorInPlatform(sparkSession, endDate, locale)
        userBehaviorInPlatform.write.mode("overwrite").parquet(s"$dwd_appstore_recommend_user_offline_behavior_1d/locale=${locale2locale(locale)}/tag=plat/date=$endDate")
        alterTableIPartitionInAlpha(sparkSession, locale2catalog(locale, "hive"), "appstore", "dwd_appstore_recommend_user_offline_behavior_1d", Map("tag" -> "plat", "date" -> endDate, "locale" -> locale2locale(locale)))

    }

    def getUserBehaviorInPlatform(sparkSession: SparkSession, endDate: String, locale: String): DataFrame = {
        import sparkSession.implicits._
        val front30Date = getFrontDay(endDate, 30)
        val front7Date = getFrontDay(endDate, 7)
        val front3Date = getFrontDay(endDate, 3)

        val orgDf = readParquet4dwd_app_stat_event_di(sparkSession, front30Date, endDate, locale)
            .where(filterLocale("region", locale))
            .where(s"event_type in ('download','install','first_launch')")
            .selectExpr("imeimd5 as id", "package_name", "event_type as behaviorType", "date")
            .repartition(1000)
            .cache()
        val downloadDf = orgDf
            .where(s"date between $front30Date and $endDate")
            .where("behaviorType = 'download'")
            .groupBy("id").agg(collect_set("package_name").as("downloadInPlat"))
        val installDf = orgDf
            .where(s"date between $front7Date and $endDate")
            .where("behaviorType = 'install'")
            .groupBy("id").agg(collect_set("package_name").as("installInPlat"))
        val activeDf = orgDf
            .where(s"date between $front7Date and $endDate")
            .where("behaviorType = 'first_launch'")
            .groupBy("id").agg(collect_set("package_name").as("activateInPlat"))
        val actionDf = downloadDf
            .join(installDf, Seq("id"), "outer")
            .join(activeDf, Seq("id"), "outer")

        val useDf = readParquet4dwm_app_usage_di(sparkSession, front3Date, endDate, locale)
            .where(filterLocale("region", locale))
            .selectExpr("imeimd5 as id", "package_name", "frequency", "duration")
            .groupBy("id", "package_name").agg(avg("frequency").as("frequency"), avg("duration").as("duration"))
            .where("id is not null  and package_name is not null and frequency > 0 and duration > 0")
            .rdd
            .map(r => (r.getString(0), (r.getString(1), r.getDouble(2), r.getDouble(3))))
            .groupByKey()
            .map(r => {
                val id = r._1
                val appList: Array[String] = r._2.toArray.sortBy(x => (x._1, 5 * x._3 + 3 * x._2)).map(x => x._1).slice(0, 10)
                (id, appList)
            })
            .toDF("id", "useInPlat")
            .repartition(1000)
        actionDf.join(useDf, Seq("id"), "outer")
    }

}
