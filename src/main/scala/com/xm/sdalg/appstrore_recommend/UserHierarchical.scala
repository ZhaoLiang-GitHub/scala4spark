package com.xm.sdalg.appstrore_recommend

import com.xm.sdalg.appstrore_recommend.Utils._
import com.xm.sdalg.commons.ClusterUtils._
import com.xm.sdalg.commons.HDFSUtils._
import com.xm.sdalg.commons.SparkContextUtils.createSparkSession
import com.xm.sdalg.commons.TimeUtils._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{avg, lit}

import scala.collection.mutable.ArrayBuffer

/**
 * @author zhaoliang6 on 20220914
 *         海外商店推荐 用户标签写入doc
 */
object UserHierarchical {
    def main(args: Array[String]): Unit = {
        val Array(locale: String, startDate: String, endDate: String, frequencyGame: String, durationGame: String) = args
        val sparkSession: SparkSession = createSparkSession(this.getClass.getSimpleName)
        var resultDf: DataFrame = null
        val glideUser = getGlideUser(sparkSession, locale, endDate)
        val heavyUser = getHeavyUser(sparkSession, locale, endDate)
        val activeUser = getIsActiveUser(sparkSession, locale, endDate)
        resultDf = glideUser
            .join(heavyUser, Seq("id"), "outer")
            .join(activeUser, Seq("id"), "outer")
            .where(filterGaid("id"))
        if (localeInCluster(locale, singapore)) {
            val discoverUser = getDiscoverprofileDi(sparkSession, locale)
            val SlevelUser = getSLevelUser(sparkSession)
            //            val sensitiveUser = getSensitiveUser(sparkSession, locale)
            resultDf = resultDf
                .join(SlevelUser, Seq("id"), "outer")
                .join(discoverUser, Seq("id"), "outer")
            //                .join(sensitiveUser, Seq("id"), "outer")
        }

        resultDf.show(false)
        resultDf.printSchema()
        resultDf.write.mode("overwrite").parquet(s"$dwd_appstore_recommend_user_hierarchical_1d/locale=${locale2locale(locale)}/date=$endDate")
        alterTableIPartitionInAlpha(sparkSession, locale2catalog(locale, "hive"), "appstore", "dwd_appstore_recommend_user_hierarchical_1d", Map("date" -> endDate, "locale" -> locale2locale(locale)))
    }

    /**
     * 游戏用户
     */
    def getGameUser(sparkSession: SparkSession, locale: String, startDate: String, endDate: String, frequencyGame: Double, durationGame: Double): DataFrame = {
        val userDf = readParquet4dwm_app_usage_di(sparkSession, startDate, endDate, locale)
            .where(filterLocale("region", locale))
            .where(filterGaid("imeimd5"))
            .selectExpr("imeimd5 as id", "package_name", "frequency", "duration")
            .join(package2intlCategoryId(sparkSession, locale), Seq("package_name"))
            .filter(row => row.getAs[Long]("intl_category_id").toInt >= 100)
            .groupBy("id").agg(avg("frequency").as("frequency"), avg("duration").as("duration"))
            .selectExpr("id", "frequency as frequencyGame", "duration as durationGame")
            .where(s"frequencyGame >= ${frequencyGame} or durationGame >= ${durationGame}")
            .withColumn("isGameUser", lit(1))
            .selectExpr("id", "isGameUser")
        println(s"游戏用户的量级是 ${userDf.where("isGameUser = 1").count()}")
        userDf.show(false)
        userDf
    }

    /**
     * 下滑用户
     */
    def getGlideUser(sparkSession: SparkSession, locale: String, date: String): DataFrame = {
        readTableInAlphaBetweenDate(sparkSession, locale2catalog(locale, "hive"), "dw", "dwd_appstore_client_track", date, date)
            .where(filterLocale("lo", locale))
            .where(filterGaid("gaid"))
            .createOrReplaceTempView("dwd_appstore_client_track_getGlideUser")
        val userDf = sparkSession.sql(
            s"""
               |SELECT
               |    gaid as id,
               |    case
               |        when down_pos>=3 then 1
               |        else 0
               |    end isGlideUser,
               |    case
               |        when down_pos<3 then 1
               |        else 0
               |    end isNoGlideUser
               |from
               |    (
               |        SELECT
               |            gaid,
               |            max(if (cur_card_type='recApps', cur_card_pos, 0)) AS down_pos
               |        FROM
               |            dwd_appstore_client_track_getGlideUser
               |        WHERE
               |            (
               |                launch_ref is not null
               |                and launch_ref!=''
               |                and launch_ref not in ('com.android.browser.global_update')
               |            )
               |            or cur_page_ref in ('miniCard', 'detailCard')
               |        GROUP BY
               |            gaid
               |    )
               |""".stripMargin)

        println(
            s"""
               |下滑用户的量级是 ${userDf.where("isGlideUser = 1").count()}
               |非下滑用户的量级是 ${userDf.where("isNoGlideUser = 1").count()}
               |""".stripMargin)
        userDf.show(false)
        userDf
    }

    /**
     * 轻中重度用户
     */
    def getHeavyUser(sparkSession: SparkSession, locale: String, date: String) = {
        readTableInAlphaBetweenDate(sparkSession, locale2catalog(locale, "hive"), "dw", "dwd_appstore_client_track", date, date)
            .where(filterLocale("lo", locale))
            .where(filterGaid("gaid"))
            .createOrReplaceTempView("dwd_appstore_client_track_getHeavyUser")
        val userDf = sparkSession.sql(
            """
              |SELECT
              |    gaid as id,
              |    case
              |        when duration<6 then 1
              |        else 0
              |    end as isLightUser,
              |    case
              |        when duration BETWEEN 6 and 20  then 1
              |        else 0
              |    end as isMiddleUser,
              |    case
              |        when duration>20 then 1
              |        else 0
              |    end as isHeavyUser
              |from
              |    (
              |        SELECT
              |            gaid,
              |            sum(if (action_type='page_end', duration, 0))/1000 as duration
              |        FROM
              |            dwd_appstore_client_track_getHeavyUser
              |        WHERE
              |            (
              |                launch_ref is not null
              |                and launch_ref!=''
              |                and launch_ref not in ('com.android.browser.global_update')
              |            )
              |            or cur_page_ref in ('miniCard', 'detailCard')
              |        GROUP BY
              |            gaid
              |    )
              |""".stripMargin)
        println(
            s"""
               |轻度用户的量级是 ${userDf.where("isLightUser = 1 ").count()}
               |中度用户的量级是 ${userDf.where("isMiddleUser = 1 ").count()}
               |重度用户的量级是 ${userDf.where("isHeavyUser =  1 ").count()}
               |""".stripMargin)
        userDf.show(false)
        userDf
    }


    def getIsActiveUser(sparkSession: SparkSession, locale: String, date: String): DataFrame = {
        readParquet4dwd_appstore_client_track(sparkSession, date, date, locale)
            .where(filterLocale("lo", locale))
            .createOrReplaceTempView("dwd_appstore_client_track_active")
        val userDf = sparkSession.sql(
            """
              |SELECT
              |    gaid as id,
              |    case
              |        WHEN open_pattern=1 THEN 1
              |    end isActiveUser,
              |    CASE
              |        WHEN open_pattern!=1 THEN 1
              |    end isPassiveUser
              |from
              |    (
              |        SELECT
              |            gaid,
              |            max(if (launch_ref='com.miui.home', 1, 0)) as open_pattern
              |        FROM
              |            dwd_appstore_client_track_active
              |        WHERE
              |            (
              |                launch_ref is not null
              |                and launch_ref!=''
              |                and launch_ref not in ('com.android.browser.global_update')
              |            )
              |            or cur_page_ref in ('miniCard', 'detailCard')
              |        GROUP BY
              |            gaid
              |    )
              |""".stripMargin)
        println(
            s"""
               |主动用户数量 ${userDf.where("isActiveUser = 1").count()}
               |被动用户数量 ${userDf.where("isPassiveUser = 1").count()}
               |""".stripMargin)
        userDf.show(false)
        userDf
    }


    def getSLevelUser(sparkSession: SparkSession): DataFrame = {
        val orgDf = sparkSession.read.parquet(getLatestExistPath(sparkSession, "/user/h_data_platform/platform/appstore/ads_user_level_alsg_7d/date=yyyyMMdd", getCurrentDateStr()))
        val high = orgDf
            .where(s"usage_level = 'high'")
            .withColumn("isSHighUser", lit(1))
            .selectExpr("gaid as id", "isSHighUser")
            .dropDuplicates("id")
        val mid = orgDf
            .where(s"usage_level = 'mid'")
            .withColumn("isSMidUser", lit(1))
            .selectExpr("gaid as id", "isSMidUser")
            .dropDuplicates("id")

        val low = orgDf
            .where(s"usage_level = 'low'")
            .withColumn("isSLowUser", lit(1))
            .selectExpr("gaid as id", "isSLowUser")
            .dropDuplicates("id")

        println(
            s"""
               |ads_user_level_alsg_7d S级高度使用用户数量 ${high.count()}
               |ads_user_level_alsg_7d S级中度使用用户数量 ${mid.count()}
               |ads_user_level_alsg_7d S级轻度使用用户数量 ${low.count()}
               |""".stripMargin)
        val result = high.join(mid, Seq("id"), "outer").join(low, Seq("id"), "outer")
        result.show(false)
        result.printSchema()
        result
    }

    def getDiscoverprofileDi(sparkSession: SparkSession, locale: String) = {
        val orgDf = sparkSession.read.parquet(getLatestExistPath(sparkSession, "/user/h_data_platform/platform/dw/dim_appstore_discover_profile_di/date=yyyyMMdd", getCurrentDateStr()))
            .where(filterLocale("lo", locale))
            .selectExpr("gaid", "user_level")
        val high = orgDf
            .where(s"user_level = 'high'")
            .withColumn("isDiscoverHighUser", lit(1))
            .selectExpr("gaid as id", "isDiscoverHighUser")
            .dropDuplicates("id")
        val mid = orgDf
            .where(s"user_level = 'middle'")
            .withColumn("isDiscoverMidUser", lit(1))
            .selectExpr("gaid as id", "isDiscoverMidUser")
            .dropDuplicates("id")

        val low = orgDf
            .where(s"user_level = 'low'")
            .withColumn("isDiscoverLowUser", lit(1))
            .selectExpr("gaid as id", "isDiscoverLowUser")
            .dropDuplicates("id")

        println(
            s"""
               |dim_appstore_discover_profile_di S级高度使用用户数量 ${high.count()}
               |dim_appstore_discover_profile_di S级中度使用用户数量 ${mid.count()}
               |dim_appstore_discover_profile_di S级轻度使用用户数量 ${low.count()}
               |""".stripMargin)
        val result = high.join(mid, Seq("id"), "outer").join(low, Seq("id"), "outer")
        result.show(false)
        result.printSchema()
        result
    }

    /**
     * 获取敏感用户
     */
    def getSensitiveUser(sparkSession: SparkSession, locale: String) = {
        import sparkSession.implicits._
        val sensitiveSet: Broadcast[Set[String]] = getAuditBlackSet(sparkSession, locale)
        println(s"敏感应用数量 ${sensitiveSet.value.size}")
        // 读取用户行为数据，获取用户安装的敏感应用，过滤出敏感用户
        val orgDf = sparkSession.read.parquet(getLatestExistPath(sparkSession, s"${dwd_appstore_recommend_user_behavior_1d}/locale=${locale2locale(locale)}/date=yyyyMMdd", getCurrentDateStr()))
            .selectExpr("id", "activateInGa", "downloadInPlat", "installInPlat", "activateInPlat", "download_pkg")
            .rdd
            .map(row => {
                val id = row.getAs[String]("id")
                val packages = new ArrayBuffer[String]()
                Range(1, 6).foreach(i => {
                    if (!row.isNullAt(i)) {
                        val pkg = row.getSeq[String](i)
                        packages ++= pkg
                    }
                })
                (id, packages.toSet)
            })
            .mapValues(_.intersect(sensitiveSet.value))
            .filter(_._2.nonEmpty)
            .toDF("id", "sensitivePkgSet")
            .withColumn("sensitiveUser", lit(1))
        orgDf.show(false)
        println(
            s"""
               |敏感用户数量 ${orgDf.where("sensitiveUser = 1").count()}
               |平均每个用户安装敏感应用数量 ${orgDf.rdd.map(_.getSeq[String](1).size).collect().sum / orgDf.count().toDouble}
               |一个用户安装敏感应用数量最大值 ${orgDf.rdd.map(_.getSeq[String](1).size).collect().max}
               |一个用户安装敏感应用数量最小值 ${orgDf.rdd.map(_.getSeq[String](1).size).collect().min}
               |""".stripMargin
        )
        orgDf
            .selectExpr("id", "sensitiveUser", "sensitivePkgSet")
    }
}
