package com.xm.sdalg.appstrore_recommend

import com.xm.sdalg.appstrore_recommend.Utils._
import com.xm.sdalg.commons.HDFSUtils._
import com.xm.sdalg.commons.SparkContextUtils.createSparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.xm.sdalg.commons.ClusterUtils._

import scala.collection.mutable.ArrayBuffer

/**
 * 在GA中的行为数据
 */
object UserBehaviorInGaV3 {
    def main(args: Array[String]): Unit = {
        val Array(locale: String, clickStartDate: String, downloadStartDate: String, installStartDate: String, activateStartDate: String, endDate: String) = args
        val sparkSession: SparkSession = createSparkSession(this.getClass.getSimpleName)
        val userBehaviorInGa: DataFrame = getUserBehaviorInGa(sparkSession, clickStartDate, downloadStartDate, installStartDate, activateStartDate, endDate, locale)
        userBehaviorInGa.write.mode("overwrite").parquet(s"/user/h_data_platform/platform/appstore/dwd_appstore_user_behavior_v2/locale=${locale2locale(locale)}/date=$endDate")
        userBehaviorInGa.show(false)
        userBehaviorInGa.printSchema()
        alterTableIPartitionInAlpha(sparkSession, locale2catalog(locale, "hive"), "appstore", "dwd_appstore_user_behavior_v2", Map("date" -> endDate))
    }

    //    behaviorType = unknown data should be discarded
    def getUserBehaviorInGa(sparkSession: SparkSession, clickStartDate: String, downloadStartDate: String, installStartDate: String, activateStartDate: String, endDate: String, locale: String): DataFrame = {
        //        format: 20220607 : year-month-day
        val dateList = List(clickStartDate.toInt, downloadStartDate.toInt, installStartDate.toInt, activateStartDate.toInt)
        val minStartDate = dateList.min.toString
        val pkg2CatDf = package2intlCategoryId(sparkSession, locale)
        val orgDf = readUserBehaviorRawData(sparkSession, minStartDate, endDate, locale)
            .join(pkg2CatDf.selectExpr("package_name as packageName", "intl_category_id as intlCategoryId"), Seq("packageName"), "left")
            .repartition(1000)
            .cache()

        val clickOrgDf = orgDf
            .where(s"date between $clickStartDate and $endDate")
            .where("behaviorType = 'DETAIL'")
            .selectExpr("id", "packageName", "intlCategoryId")

        val (clickDf, clickAppDf, clickGameDf) = getClickDownloadDf(clickOrgDf, "click")

        val downloadOrgDf = orgDf
            .where(s"date between $downloadStartDate and $endDate")
            .where("behaviorType in ('DOWNLOAD_COMPLETE','DOWNLOAD')")
            .selectExpr("id", "packageName", "intlCategoryId")

        val (downloadDf, downloadAppDf, downloadGameDf) = getClickDownloadDf(downloadOrgDf, "download")

        val clickDownloadAppInGa = clickAppDf
            .join(downloadAppDf, Seq("id"), "outer")
            .withColumn("clickDownloadAppInGa", array_distinct(split(concat_ws(",", col("clickAppInGa"), col("downloadAppInGa")), ",")))
            .select("id", "clickDownloadAppInGa")


        val clickDownloadGameInGa = clickGameDf
            .join(downloadGameDf, Seq("id"), "outer")
            .withColumn("clickDownloadGameInGa", array_distinct(split(concat_ws(",", col("clickGameInGa"), col("downloadGameInGa")), ",")))
            .select("id", "clickDownloadGameInGa")


        val installDf = orgDf
            .where(s"date between $installStartDate and $endDate")
            .where("behaviorType = 'INSTALL_COMPLETE'")
            .selectExpr("id", "packageName")
            .groupBy("id").agg(collect_set("packageName").as("installInGa"))

        val activateDf = orgDf
            .where(s"date between $activateStartDate and $endDate")
            .where("behaviorType = 'ACTIVE'")
            .selectExpr("id", "packageName")
            .groupBy("id").agg(collect_set("packageName").as("activateInGa"))


        var actionDf = clickDf
            .join(downloadDf, Seq("id"), "outer")
            .join(installDf, Seq("id"), "outer")
            .join(activateDf, Seq("id"), "outer")
            .join(clickDownloadAppInGa, Seq("id"), "outer")
            .join(clickDownloadGameInGa, Seq("id"), "outer")

        // 俄罗斯集群中缺少的用户在GA内的离线行为
        if (localeInCluster(locale, russia)) {
            import sparkSession.implicits._
            val russiaOrgDf = readParquet4dwm_appstore_user_behavior_di(sparkSession, locale, minStartDate, endDate)
                .where(filterLocale("locale", locale))
                .where("id.id is not null and packageName is not null")
                .where("behaviorType in ('DOWNLOAD_COMPLETE','DOWNLOAD','DETAIL')")
                .selectExpr("id.id as id", "packageName", "behaviorType", "date", "timestamp")
                .join(pkg2CatDf.selectExpr("package_name as packageName", "intl_category_id as intlCategoryId"), Seq("packageName"))

            def russiaUserAction(df: DataFrame) = {
                df
                    .selectExpr("id", "packageName", "behaviorType", "timestamp")
                    .dropDuplicates("id", "packageName", "behaviorType")
                    .rdd
                    .map(row => (row.getString(0), (row.getString(1), row.getString(2), row.getLong(3))))
                    .groupByKey()
                    .map(row => {
                        val id = row._1
                        val download = new ArrayBuffer[String]()
                        val play = new ArrayBuffer[String]()
                        row._2.foreach({ case (packageName, behaviorType, timestamp) =>
                            behaviorType match {
                                case "DOWNLOAD_COMPLETE" | "DOWNLOAD" => download.append(s"${packageName}&${timestamp}")
                                case "DETAIL" => play.append(s"${packageName}&${timestamp}")
                            }
                        })
                        (id, download.mkString(","), play.mkString(","))
                    })
            }

            val russiaGame = russiaUserAction(russiaOrgDf.where("intlCategoryId > 100"))
                .toDF("id", "UGDownloadSeq", "deepPlayGameSeq")
            val russiaApp = russiaUserAction(russiaOrgDf.where("0 < intlCategoryId and intlCategoryId < 100"))
                .toDF("id", "UADownloadSeq", "deepPlayAppSeq")
            actionDf = actionDf
                .join(russiaApp, Seq("id"), "outer")
                .join(russiaGame, Seq("id"), "outer")
        }
        actionDf
    }

    def readUserBehaviorRawData(sparkSession: SparkSession, startDate: String, endDate: String, locale: String): DataFrame = {
        var result = readParquet4dwm_appstore_user_behavior_di(sparkSession, locale, startDate, endDate)
            .where(filterLocale("locale", locale))
            .where("id.id is not null and packageName is not null")
            .where("behaviorType in ('ACTIVE','INSTALL_COMPLETE','DOWNLOAD_COMPLETE','DOWNLOAD','DETAIL')")
            .selectExpr("id.id as id", "packageName", "behaviorType", "date")
        val blackUserDf = result
            .selectExpr("id", "behaviorType", "packageName")
            .groupBy("id").agg(countDistinct("packageName").as("num"))
            .where("num > 300")
            .selectExpr("id")
        result = result
            .join(blackUserDf, Seq("id"), "leftanti")
        result
    }

    //    def getOutlierFilter(sparkSession: SparkSession, df: DataFrame): Broadcast[Set[String]] = {
    //        val largeClickApp = sparkSession.sparkContext.broadcast(df
    //          .groupBy("packageName","id.id")
    //          .agg(count("*").as("id_count"))
    //          .filter("id_count > 50") //去除单个用户点击次数>50的app
    //          .rdd
    //          .map(r => r.getAs[String]("packageName"))
    //          .collect()
    //          .toSet
    //        )
    //
    //        largeClickApp
    //    }


    def getClickDownloadDf(orgDf: DataFrame, category: String): (DataFrame, DataFrame, DataFrame) = {

        val totalDf = orgDf
            .groupBy("id").agg(collect_set("packageName").as(s"$category" + "InGa"))

        val appDf = orgDf
            .where("intlCategoryId between 0 and 100")
            .groupBy("id").agg(collect_set("packageName").as(s"$category" + "AppInGa"))

        val gameDf = orgDf
            .where("intlCategoryId > 100")
            .groupBy("id").agg(collect_set("packageName").as(s"$category" + "GameInGa"))

        (totalDf, appDf, gameDf)
    }


}
