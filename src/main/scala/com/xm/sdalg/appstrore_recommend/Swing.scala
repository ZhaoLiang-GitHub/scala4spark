package com.xm.sdalg.appstrore_recommend

import com.xm.sdalg.appstrore_recommend.Utils._
import com.xm.sdalg.commons.CalcUtils._
import com.xm.sdalg.commons.ClusterUtils._
import com.xm.sdalg.commons.CollaborativeFilteringUtils._
import com.xm.sdalg.commons.HDFSUtils._
import com.xm.sdalg.commons.SparkContextUtils._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, sum}

/**
 * @author zhaoliang6 on 20220628
 *         海外商店推荐 swing 召回
 */
object Swing {
    def main(args: Array[String]): Unit = {
        val Array(locale, startDate, endDate, version: String) = args
        val sparkSession: SparkSession = createSparkSession(this.getClass.getSimpleName)
        import sparkSession.implicits._
        val recallBlackSet = getRecallBlackSet(sparkSession, locale, endDate)
        val userItemScoreDfV2 = getQueryItemScoreDf(sparkSession, locale, startDate, endDate)
        val allAppFilterSAB_CDV7 = getAppSetBc(sparkSession, locale, endDate, "SAB", "CD")

        val i2iSimDf = swing(sparkSession, userItemScoreDfV2, minCoUser = 0, version = "V1")
        val resultDf = getI2IMapBySim(i2iSimDf, blackItemSet = recallBlackSet, whiteItemSet = allAppFilterSAB_CDV7).toDF("id", "swing_query_V1")

        resultDf.show(false)
        resultDf.printSchema()
        resultDf.write.mode("overwrite").save(s"$dwd_appstore_recommend_swing_i2i_1d/locale=${locale2locale(locale)}/tag=query/version=V1/date=$endDate")
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

}
