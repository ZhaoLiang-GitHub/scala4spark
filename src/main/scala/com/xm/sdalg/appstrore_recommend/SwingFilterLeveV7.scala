package com.xm.sdalg.appstrore_recommend

import com.xm.sdalg.appstrore_recommend.Utils._
import com.xm.sdalg.commons.CalcUtils._
import com.xm.sdalg.commons.CollaborativeFilteringUtils._
import com.xm.sdalg.commons.SparkContextUtils._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._
import com.xm.sdalg.commons.ClusterUtils._
import com.xm.sdalg.commons.HDFSUtils._
import org.apache.spark.sql.functions.{asc, col, desc, sum}

/**
 * @author zhaoliang6 on 20220628
 *         海外商店推荐 swing 召回
 */
object SwingFilterLeveV7 {
    def main(args: Array[String]): Unit = {
        val Array(locale, startDate, endDate, front7DayLevel, front1DayLevel) = args
        val sparkSession: SparkSession = createSparkSession(this.getClass.getSimpleName)
        import sparkSession.implicits._
        val allAppFilterSAB_CDV7 = getAppSetBc(sparkSession, locale, endDate, front7DayLevel, front1DayLevel)
        val recallBlackSet = getRecallBlackSet(sparkSession, locale, endDate)
        val userItemScoreDfV2 = getQueryItemScoreDf(sparkSession, locale, startDate, endDate)

        val i2iSimDf = swing(sparkSession, userItemScoreDfV2, minCoUser = 0, version = "V1")
        val resultDf = getI2IMapBySim(i2iSimDf, whiteItemSet = allAppFilterSAB_CDV7, blackItemSet = recallBlackSet).toDF("id", s"swing_filter_${front7DayLevel}_${front1DayLevel}_V7")

        resultDf.show(false)
        resultDf.printSchema()
        resultDf.write.mode("overwrite").save(s"$dwd_appstore_recommend_swing_i2i_1d/locale=${locale2locale(locale)}/tag=query_filter_level_V7/version=${front7DayLevel}_${front1DayLevel}/date=$endDate")
    }

    def getQueryItemScoreDf(sparkSession: SparkSession, locale: String, startDate: String, endDate: String): DataFrame = {
        val queryToScoreDf = readTableInAlphaBetweenDate(sparkSession, locale2catalog(locale, "hive"), "dw", "dwm_appstore_mipicks_search_package_v2_di", startDate, endDate)
            .where(s"keyword is not null and package_name is not null")
            .groupBy("keyword", "package_name")
            .agg(sum("exposure_uv").as("EXPOSURE_UV"), sum("download_uv").as("DOWNLOAD_UV"))
            .where(s"EXPOSURE_UV is not null and DOWNLOAD_UV is not null")
            .withColumn("score", wilsonCtrUDF(col("DOWNLOAD_UV"), col("EXPOSURE_UV")))
            .selectExpr("keyword as user", "package_name as item", "score")
            .where("user is not null and user != '' and item != '' and score != 0")
        queryToScoreDf
    }

    def getQueryItemScoreDfV2(sparkSession: SparkSession, locale: String, startDate: String, endDate: String, tiems: Int = 1): DataFrame = {
        var queryToScoreDf = readTableInAlphaBetweenDate(sparkSession, locale2catalog(locale, "hive"), "dw", "dwm_appstore_mipicks_search_package_v2_di", startDate, endDate)
            .where(s"keyword is not null and package_name is    not null")
            .groupBy("keyword", "package_name")
            .agg(sum("exposure_uv").as("EXPOSURE_UV"), sum("download_uv").as("DOWNLOAD_UV"))
            .selectExpr("keyword as user", "package_name as item", s"DOWNLOAD_UV * ${tiems} as score")
        val whiteUser = queryToScoreDf
            .groupBy("user").count()
            .where("1 < count and count < 100")
            .select("user")
        queryToScoreDf = queryToScoreDf.join(whiteUser, Seq("user"))
        val whiteItem = queryToScoreDf
            .groupBy("item").count()
            .where("1 < count and count < 10000")
            .select("item")
        val reuslt = queryToScoreDf.join(whiteItem, Seq("item"))
            .where("user is not null and item is not null and user != '' and item != ''")
            .where("score > 0")
            .selectExpr("user", "item", "score")
            .repartition(1000)
        reuslt
    }

    def getQueryItemScoreDfV9(sparkSession: SparkSession, locale: String, startDate: String, endDate: String, tiems: Int = 1): DataFrame = {
        var queryToScoreDf = readTableInAlphaBetweenDate(sparkSession, locale2catalog(locale, "hive"), "dw", "dwm_appstore_mipicks_search_package_v2_di", startDate, endDate)
            .where(filterLocale("lo", locale))
            .where(s"keyword is not null and package_name is not null")
            .groupBy("keyword", "package_name").agg(
            sum("exposure_uv").as("exposure_uv"),
            sum("click_uv").as("click_uv"),
            sum("download_uv").as("download_uv"),
            sum("install_uv").as("install_uv")
        )
            .withColumn("click_rate", wilsonCtrUDF(col("click_uv"), col("exposure_uv")))
            .withColumn("download_rate", wilsonCtrUDF(col("download_uv"), col("exposure_uv")))
            .withColumn("install_rate", wilsonCtrUDF(col("install_uv"), col("exposure_uv")))
            .withColumn("score", col("click_rate") * 10 + col("download_rate") * 50 + col("install_rate") * 100)
            .where("score > 0")
            .selectExpr("keyword as user", "package_name as item", s"score * ${tiems} as score")
        val whiteUser = queryToScoreDf
            .groupBy("user").count()
            .where("1 < count and count < 100")
            .select("user")
        val whiteItem = queryToScoreDf
            .groupBy("item").count()
            .where("1 < count and count < 10000")
            .select("item")
        val reuslt = queryToScoreDf
            .join(whiteUser, Seq("user"))
            .join(whiteItem, Seq("item"))
            .where("user is not null and item is not null and user != '' and item != ''")
            .where("0 < score ")
            .selectExpr("user", "item", "score")
            .repartition(1000)
        reuslt
    }

    def getQueryItemScoreDfV6(sparkSession: SparkSession, locale: String, startDate: String, endDate: String, tiems: Int = 1): DataFrame = {
        val queryToScoreDf = readTableInAlphaBetweenDate(sparkSession, locale2catalog(locale, "hive"), "dw", "dwm_appstore_mipicks_search_package_v2_di", startDate, endDate)
            .where(s"keyword is not null and package_name is not null")
            .groupBy("keyword", "package_name")
            .agg(sum("exposure_uv").as("EXPOSURE_UV"), sum("download_uv").as("DOWNLOAD_UV"))
            .selectExpr("keyword as user", "package_name as item", s"DOWNLOAD_UV * ${tiems} as score")
        val blackItem = queryToScoreDf
            .groupBy("item").count()
            .where("count > 10000")
            .select("item")
            .distinct()
            .rdd
            .map(_.getString(0))
            .collect()
            .toSet
        val blackItemRow = queryToScoreDf
            .filter(row => blackItem.contains(row.getAs[String]("item")))
            .sample(0.05)
        val whiteUser = queryToScoreDf
            .groupBy("user").count()
            .where("1 < count and count < 100")
            .select("user")
            .distinct()
        val whiteItem = queryToScoreDf
            .groupBy("item").count()
            .where("1 < count and count < 10000")
            .select("item")
            .distinct()
        val whiteRow = queryToScoreDf
            .join(whiteUser, Seq("user"))
            .join(whiteItem, Seq("item"))

        whiteRow
            .unionByName(blackItemRow)
            .groupBy("user", "item").agg(sum("score").as("score"))
            .selectExpr("user", "item", "score")
            .where("user is not null and item is not null")
            .where("score > 0")
    }
}
