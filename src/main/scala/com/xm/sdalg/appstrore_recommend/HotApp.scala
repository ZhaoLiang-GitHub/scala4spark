package com.xm.sdalg.appstrore_recommend

import com.xm.sdalg.appstrore_recommend.Utils._
import com.xm.sdalg.commons.HDFSUtils._
import com.xm.sdalg.commons.SparkContextUtils.createSparkSession
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.xm.sdalg.commons.ClusterUtils._
import org.apache.spark.rdd.RDD

/**
 * @author zhaoliang6 on 20220718
 *         从旧仓库中复制过来的热门数据的生成方式
 *         20220726 热门判别方式从pv都修改到uv
 */
object HotApp {
    def main(args: Array[String]): Unit = {
        val Array(locale: String, startDate: String, endDate: String) = args
        val sparkSession: SparkSession = createSparkSession(this.getClass.getSimpleName)
        var resultDf: Dataset[Row] = null
        if (locale == "ID") {
            // 20230726 第二版评级分数在印尼推全，基线从V7更改到V9
            val allAppFilterSAB_CDV9 = getAppSetBc(sparkSession, locale, endDate, "SAB", "CD")
            resultDf = union(resultDf, getHotInSearch(sparkSession, startDate, endDate, locale, version = s"SAB_CD_V9", whiteSet = allAppFilterSAB_CDV9, topN = 500))
            resultDf = union(resultDf, getHotInRec(sparkSession, startDate, endDate, locale, version = s"SAB_CD_V9", whiteSet = allAppFilterSAB_CDV9, topN = 500))

        } else {
            val allAppFilterSAB_CV7 = getAppSetBc(sparkSession, locale, endDate, "SAB", "C")
            resultDf = union(resultDf, getHotInSearch(sparkSession, startDate, endDate, locale, version = s"SAB_C_V7", whiteSet = allAppFilterSAB_CV7, topN = 500))
            resultDf = union(resultDf, getHotInRec(sparkSession, startDate, endDate, locale, version = s"SAB_C_V7", whiteSet = allAppFilterSAB_CV7, topN = 500))
        }

        resultDf.show(100, false)
        resultDf.printSchema()
        rddSaveInHDFS(sparkSession, resultDf.toJSON.rdd, dim_appstore_recommend_tag_doc(endDate, locale2locale(locale), "hotItemByOldWay"))


    }

    /**
     * 在推荐中根据downloadUv获得热门应用、热门游戏、热门类别
     */
    def getHotInRec(sparkSession: SparkSession,
                    startDate: String, endDate: String,
                    locale: String, topN: Int = 200,
                    version: String,
                    whiteSet: Broadcast[Set[String]]
                   ): DataFrame = {
        val orgDf = readParquet4dwm_appstore_user_behavior_di(sparkSession, locale, startDate, endDate)
            .where(filterLocale("locale", locale))
            .where(s"packageName is not null")
            .where("behaviorType = 'DOWNLOAD_COMPLETE'")
            .selectExpr("id.id as id", "packageName", "date")
            .distinct()
            .groupBy("packageName").agg(count("id").as("count")) // downloadUV
            .join(package2intlCategoryId(sparkSession, locale).selectExpr("package_name as packageName ", "intl_category_id as intlCategoryId"), Seq("packageName"))
            .selectExpr("packageName", "count as downloadPv", "intlCategoryId")
            .filter(row => whiteSet.value.contains(row.getString(0)))
            .repartition(1000)
            .cache()
        val hotApps = getTopItemByDescScore(sparkSession, locale,
            orgDf.where(s"intlCategoryId > 0 and intlCategoryId < 100").selectExpr("packageName as item", "downloadPv as score"),
            "hotAppsInRec" + version, topN)
        val hotGames = getTopItemByDescScore(sparkSession, locale,
            orgDf.where(s"intlCategoryId >= 100").selectExpr("packageName as item", "downloadPv as score"),
            "hotGamesInRec" + version, topN)
        val hotCate = getTopItemByDescScore(sparkSession, locale,
            orgDf.groupBy("intlCategoryId").agg(sum("downloadPv").as("score")).selectExpr("cast(intlCategoryId as string) as item", "score"),
            "hotCateInRec" + version, 13)
        val resultDf = hotApps.unionByName(hotGames).unionByName(hotCate)
        resultDf
    }

    /**
     * 搜索中的曝光前100+下载前100去重
     */
    def getHotInSearch(sparkSession: SparkSession,
                       startDate: String, endDate: String,
                       locale: String, topN: Int = 100,
                       version: String,
                       whiteSet: Broadcast[Set[String]]): DataFrame = {
        import sparkSession.implicits._
        val userDf = readTableInAlphaBetweenDate(sparkSession, locale2catalog(locale, "hive"), "dw", "dwd_appstore_search_di", startDate, endDate)
            .where(filterLocale("lo", locale))
            .where(s" cur_page_type IN ('searchEntry','native_searchEntry','searchSuggest','native_searchSuggest','searchResult','native_searchResult')")
            .where(s"gaid is not null and package_name is not null")
            .where(s"action_type in ('item_exposure','download_install','item_click')")
            .selectExpr("gaid as id", "keyword as query", "package_name", "action_type", "date") // 一个人在同一天内，在一个query下，对同一个app只能有一次行为
            .distinct()
            .selectExpr("package_name as packageName", "action_type", "id")
            .filter(row => whiteSet.value.contains(row.getString(0)))
            .groupBy("packageName", "action_type").agg(count("id").as("count")) //曝光uv 下载uv
            .selectExpr("packageName as item", "action_type", "count as score")
            .repartition(1000)
            .cache()
        val topExposureDf = getTopItemByDescScore(sparkSession, locale, userDf.where("action_type = 'item_exposure'"), "topExposureInSearch" + version, topN)
        val topDownloadInSearch = getTopItemByDescScore(sparkSession, locale, userDf.where("action_type = 'download_install'"), "topDownloadInSearch" + version, topN)
        val topClickInSearch = getTopItemByDescScore(sparkSession, locale, userDf.where("action_type = 'item_click'"), "topClickInSearch" + version, topN)

        val exposure: RDD[String] = topExposureDf
            .selectExpr("apps")
            .rdd
            .flatMap(_.getMap[String, Long](0).keySet)
        val download: RDD[String] = topDownloadInSearch
            .selectExpr("apps")
            .rdd
            .flatMap(_.getMap[String, Long](0).keySet)
        val exposureAndDownload = exposure
            .union(download)
            .distinct()
            .map(row => ("topAppsInSearch" + version, row))
            .groupByKey()
            .map(row => (row._1, row._2.toArray.map(ii => (ii, 1L)).toMap))
            .toDF("id", "apps")
        var result: DataFrame = exposureAndDownload
            .unionByName(topExposureDf)
            .unionByName(topDownloadInSearch)
            .unionByName(topClickInSearch)
        result
    }

    def getTopItemByDescScore(sparkSession: SparkSession, locale: String, orgDf: DataFrame, name: String, topN: Int): DataFrame = {
        import sparkSession.implicits._
        orgDf
            .selectExpr("item", "score")
            .rdd
            .map(row => (name, (row.getString(0), row.getLong(1))))
            .groupByKey()
            .map(row => (row._1, row._2.toArray.sortBy(_._2).reverse.slice(0, topN).toMap))
            .toDF("id", "apps")
    }
}
