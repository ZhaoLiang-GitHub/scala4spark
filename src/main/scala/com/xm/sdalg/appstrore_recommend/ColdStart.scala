package com.xm.sdalg.appstrore_recommend

import com.xm.sdalg.commons.HDFSUtils._
import com.xm.sdalg.commons.TimeUtils._
import com.xm.sdalg.commons.CalcUtils.wilsonCtrUDF
import com.xm.sdalg.appstrore_recommend.Utils._
import com.xm.sdalg.commons.SparkContextUtils.createSparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, countDistinct, desc, expr, lit, sum, udf}
import com.xm.sdalg.commons.ClusterUtils._

/**
 * 基于ucb的冷启动
 */
object ColdStart {
    def main(args: Array[String]): Unit = {
        val Array(localeOrg: String, startDate: String, endDate: String) = args
        var locale = localeOrg
        if (localeInCluster(localeOrg, singapore) && localeOrg != "ID")
            locale = "ID"

        val sparkSession: SparkSession = createSparkSession(this.getClass.getSimpleName)
        import sparkSession.implicits._
        var resultDf: DataFrame = null
        val orgDf = readTableInAlphaBetweenDate(sparkSession, locale2catalog(locale, "iceberg"), "appstore", "ods_ias_client_track", yyyyMMddSplit(startDate), yyyyMMddSplit(endDate))
            .where(filterLocale("lo", localeOrg))
            .cache()
        Array("S").foreach(front7DayLevel => {
            Array("app", "game").foreach(appType => {
                val allApp = getAppSetBc(sparkSession, locale, endDate, front7DayLevel, "", cate = appType)
                val thisLevel = orgDf.filter(row => allApp.value.contains(row.getAs("package_name")))
                val allExpose = thisLevel
                    .where(s"(cur_page_type IN ('native_home_feature') OR (cur_page_type LIKE '%detail%' AND pre_page_type IN ('native_home_feature')))")
                    .where("cur_page_type not like '%detail%' and action_type = 'item_exposure'")
                    .selectExpr("gaid")
                    .distinct()
                    .count()
                val allDownload = thisLevel
                    .where(s"cur_page_type LIKE '%detail%'")
                    .where("action_type='download_install' and install_status='download_request' and install_type in ('manual_install','auto_install')")
                    .selectExpr("gaid", "package_name")
                    .distinct()
                    .count()
                var allCtr: Double = allDownload.toDouble / allExpose.toDouble
                if (allCtr.isNaN)
                    allCtr = -1D
                val avgExpose = allExpose.toDouble / allApp.value.size / 10 // 将ucb两项到同一个数量级上
                val packageExpose = thisLevel
                    .where(s"(cur_page_type IN ('native_home_feature') OR (cur_page_type LIKE '%detail%' AND pre_page_type IN ('native_home_feature')))")
                    .where("cur_page_type not like '%detail%' and action_type = 'item_exposure'")
                    .groupBy("package_name").agg(countDistinct("gaid").as("expose_uv"))
                val packageDownload = thisLevel
                    .where(s"cur_page_type LIKE '%detail%'")
                    .where("action_type='download_install' and install_status='download_request' and install_type in ('manual_install','auto_install')")
                    .groupBy("package_name").agg(countDistinct("gaid").as("download_pkg"))
                val app0expose = packageExpose
                    .where("expose_uv = 0")
                    .selectExpr("package_name")
                    .distinct()
                    .rdd
                    .map(_.getString(0))
                    .collect()
                    .map(ii => (ii, 10000D))
                    .toMap
                val appUCB = packageExpose.join(packageDownload, Seq("package_name"))
                    .where("expose_uv > 0")
                    .withColumn("ctr", col("download_pkg") / col("expose_uv"))
                    .withColumn("ucb", ucb(avgExpose)(col("ctr"), col("expose_uv")))
                    .where(s"ucb > ${allCtr}")
                    .selectExpr("package_name", "ucb")
                    .rdd
                    .map(row => (row.getString(0), row.getDouble(1)))
                    .collectAsMap()
                println(
                    s"""
                       |${front7DayLevel}级别中${appType}原有个数 ${allApp.value.size}
                       |未曝光的有${app0expose.size}个，需要通过ucb召回的有${appUCB.size}
                       |""".stripMargin)
                val tempResult: DataFrame = sparkSession.sparkContext.parallelize(
                    Array(
                        (s"ucbColdStart_${front7DayLevel}_${appType}", app0expose.++(appUCB).toArray.sortBy(_._2).reverse.slice(0, 200).toMap)
                    ))
                    .toDF("id", "apps")
                if (resultDf == null) resultDf = tempResult
                else resultDf = resultDf.unionByName(tempResult)
            })
        })
        resultDf.sort("id").show(1000, false)
        resultDf.printSchema()
        rddSaveInHDFS(sparkSession, resultDf.toJSON.rdd, dim_appstore_recommend_tag_doc(endDate, locale2locale(localeOrg), "ucbColdStart"))
    }

    def ucb(allExpose: Double): UserDefinedFunction =
        udf((ctr: Double, expose: Long) => ctr + math.log10(allExpose / expose))

    def ucbV2(allExpose: Double): UserDefinedFunction =
        udf((ctr: Double, expose: Long) => ctr + math.sqrt(math.log10(allExpose) / expose))

}
