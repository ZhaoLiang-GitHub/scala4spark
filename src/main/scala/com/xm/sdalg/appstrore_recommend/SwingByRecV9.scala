package com.xm.sdalg.appstrore_recommend

import com.xm.sdalg.appstrore_recommend.SwingFilterLeveV7._
import com.xm.sdalg.appstrore_recommend.UserHierarchical.getIsActiveUser
import com.xm.sdalg.appstrore_recommend.Utils._
import com.xm.sdalg.commons.ClusterUtils._
import com.xm.sdalg.commons.CollaborativeFilteringUtils.{getI2IMapBySim, metric4res, swing}
import com.xm.sdalg.commons.SparkContextUtils._
import com.xm.sdalg.commons.TimeUtils.getFrontDay
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, countDistinct, sum, udf}

/**
 * @author zhaoliang6 on 20220628
 *         海外商店推荐 swing 召回
 */
object SwingByRecV9 {
    def main(args: Array[String]): Unit = {
        val Array(locale, endDate, search, rec) = args
        val sparkSession: SparkSession = createSparkSession(this.getClass.getSimpleName)
        import sparkSession.implicits._
        val allAppSet = {
            if (localeInCluster(locale, singapore) && locale.equals("ID"))
                getAppSetBc(sparkSession, locale, endDate, "SAB", "CD")
            else
                getAppSetBc(sparkSession, locale, endDate, "SAB", "C")
        }

        val recallBlackSet = getRecallBlackSet(sparkSession, locale, endDate)
        val queryItemScoreV2 = getQueryItemScoreDfV9(sparkSession, locale, getFrontDay(endDate, search.toInt), endDate, 10)
        val userItemScoreDfV2 = getRecScoreV9(sparkSession, locale, getFrontDay(endDate, rec.toInt), endDate)

        val searchSim = swing(sparkSession, queryItemScoreV2, minCoUser = 0, version = "V1")
        val recSim = swing(sparkSession, userItemScoreDfV2, minCoUser = 0, version = "V1")
        val i2iSimDf = searchSim.union(recSim)
            .selectExpr("item1", "item2", "cast(sim as double) as sim")
            .rdd
            .flatMap(row => {
                val item1 = row.getAs[String]("item1")
                val item2 = row.getAs[String]("item2")
                val sim = row.getAs[Double]("sim")
                Array(
                    ((item1, item2), sim),
                    ((item2, item1), sim)
                )
            })
            .reduceByKey(_ + _)
            .map(row => (row._1._1, row._1._2, row._2))
            .toDF("item1", "item2", "sim")

        val swingByRecV3 = getI2IMapBySim(i2iSimDf, whiteItemSet = allAppSet, blackItemSet = recallBlackSet, minSimScore = 0.1).toDF("id", "swingByRecV9")
        metric4res(swingByRecV3, "id", "swingByRecV9")
        swingByRecV3.printSchema()
        swingByRecV3.show(false)
        swingByRecV3.write.mode("overwrite").save(s"$dwd_appstore_recommend_swing_i2i_1d/locale=${locale2locale(locale)}/tag=swingByRec/version=V9/date=$endDate")
    }

    def getRecScoreV9(sparkSession: SparkSession, locale: String, startDate: String, endDate: String): DataFrame = {
        def action2scoreUDF: UserDefinedFunction = {
            udf((k: String) => {
                Map(
                    "ACTIVE" -> 3,
                    "INSTALL_COMPLETE" -> 3,
                    "DOWNLOAD_COMPLETE" -> 2,
                    "DOWNLOAD" -> 2,
                    "DETAIL" -> 1,
                )(k)
            })
        }

        var orgDf = readParquet4dwm_appstore_user_behavior_di(sparkSession, locale, startDate, endDate)
            .where(filterLocale("locale", locale))
            .where("id.id is not null")
            .where("packageName is not null and packageName != ''")
            .where("behaviorType in ('ACTIVE','INSTALL_COMPLETE','DOWNLOAD_COMPLETE','DOWNLOAD')")
            .selectExpr("id.id as id", "packageName", "behaviorType", "date")
            .distinct()
        val userDf = orgDf
            .groupBy("id").agg(countDistinct("packageName").as("count"))
            .where("1 < count and count < 100")
            .selectExpr("id")
        val packageDf = orgDf
            .groupBy("packageName").agg(countDistinct("id").as("count"))
            .where(s"1 < count and count < 10000")
            .selectExpr("packageName")
        orgDf = orgDf
            .join(userDf, Seq("id"))
            .join(packageDf, Seq("packageName"))
            .join(getIsActiveUser(sparkSession, locale, endDate), Seq("id"), "left")


        val zhudong = orgDf
            .where("isActiveUser = 1 and length(id) = 36")
            .where("behaviorType in ('ACTIVE','INSTALL_COMPLETE','DOWNLOAD_COMPLETE','DOWNLOAD')")
            .withColumn("score", action2scoreUDF(col("behaviorType")) * 4)
        val beidong = orgDf
            .where("isPassiveUser = 1 or length(id) != 36")
            .where("behaviorType in ('ACTIVE','INSTALL_COMPLETE','DOWNLOAD_COMPLETE','DOWNLOAD')")
            .withColumn("score", action2scoreUDF(col("behaviorType")))
        val result = zhudong.union(beidong)
            .groupBy("id", "packageName").agg(sum("score").as("score"))
            .selectExpr("id as user", "packageName as item", "score")
            .where("score > 0")
            .where("user is not null and item is not null and user != '' and item != ''")
            .selectExpr("user", "item", "score")
            .repartition(1000)
        result
    }
}
