package com.xm.sdalg.appstrore_recommend

import com.xm.sdalg.appstrore_recommend.HotApp.{getHotInRec, getHotInSearch}
import com.xm.sdalg.appstrore_recommend.NewHot._
import com.xm.sdalg.appstrore_recommend.Utils._
import com.xm.sdalg.commons.ClusterUtils._
import com.xm.sdalg.commons.HDFSUtils.{rddSaveInHDFS, union}
import com.xm.sdalg.commons.SparkContextUtils.createSparkSession
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object HotVideoApp {
    def main(args: Array[String]): Unit = {
        val Array(locale: String, startDate: String, endDate: String) = args
        val sparkSession: SparkSession = createSparkSession(this.getClass.getSimpleName)
        var resultDf: DataFrame = null
        if (locale == "ID") {
            val allApp: Broadcast[Set[String]] = getAppSetBc(sparkSession, locale, endDate, "SAB", "CD")
            resultDf = union(resultDf, getHotVideoApp(sparkSession, locale, startDate, endDate, allApp, "hotVideoAppInPlat"))
        }
        else {
            val allApp = getAppSetBc(sparkSession, locale, endDate, "SAB", "C")
            resultDf = union(resultDf, getHotVideoApp(sparkSession, locale, startDate, endDate, allApp, "hotVideoAppInPlat"))
        }

        resultDf.show(100, false)
        rddSaveInHDFS(sparkSession, resultDf.toJSON.rdd, dim_appstore_recommend_tag_doc(endDate, locale2locale(locale), "hotVideoAppInPlat"))

    }

    def getHotVideoApp(sparkSession: SparkSession, locale: String, startDate: String, endDate: String,
                       whiteApp: Broadcast[Set[String]], name: String) = {
        import sparkSession.implicits._
        val filterWhiteApp: Broadcast[Set[String]] = sparkSession.sparkContext.broadcast(
            sparkSession.read.table(s"${locale2catalog(locale, "hive")}.appstore.intl_app_info")
                .where(
                    """
                      |video != '' and video != '{}' and get_json_object(video, '$.en_US.screenType') = 1
                      |""".stripMargin)
                .selectExpr("packagename as package_name")
                .filter(row => whiteApp.value.contains(row.getString(0)))
                .distinct()
                .rdd.map(_.getString(0)).collect().toSet
        )


        val resultDf = getDownloadNumByPlat(sparkSession, locale, startDate, endDate, "temp", filterWhiteApp)
            .selectExpr("package_name", "temp")
            .rdd
            .map(row => (row.getString(0), row.getLong(1)))
            .map(row => (name, (row._1, row._2)))
            .groupByKey()
            .map(row => (row._1, row._2.toArray.sortBy(_._2).reverse.slice(0, 200).toMap))
            .toDF("id", "apps")
        resultDf
    }
}
