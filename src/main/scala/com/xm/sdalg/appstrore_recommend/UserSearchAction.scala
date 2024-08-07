package com.xm.sdalg.appstrore_recommend

import com.xm.sdalg.appstrore_recommend.Utils._
import com.xm.sdalg.commons.SparkContextUtils.createSparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer
import com.xm.sdalg.commons.ClusterUtils._

/**
 * @author zhaoliang6 on 20220418
 *         用户的搜索行为
 */
object UserSearchAction {
    def main(args: Array[String]): Unit = {
        // 用户30天内的搜索行为下的结果 dwd_appstore_user_search_action
        // 用户7天内的搜索行为下的结果 dwd_appstore_user_search_action_7d (上实验)
        val Array(locale: String, startDate: String, endDate: String, outputPath: String) = args
        val sparkSession: SparkSession = createSparkSession(this.getClass.getSimpleName)
        import sparkSession.implicits._


        val orgDf = sparkSession.read.parquet(dwd_appstore_search_di)
            .where(s"date between $startDate and $endDate")
            .where(filterLocale("lo",locale))
            .where(s" cur_page_type IN ('searchEntry','native_searchEntry','searchSuggest','native_searchSuggest','searchResult','native_searchResult')")
            .where(s"package_name is not null")
            .where(s"action_type in ('item_click','download_install')")
            .selectExpr("gaid as id", "keyword as query", "package_name", "action_type")
            .cache()

        /**
         * 用户搜索行为与query数据join
         * 获得用户 query query全局统计做召回
         * 暂时不使用
         */
        //        val queryDf = sparkSession.read.parquet(s"/user/h_data_platform/platform/appstore/mipicks_search_query_info_v3/date=$endDate")
        //          .where(s"query is not null and rewrited_query is not null")
        //          .selectExpr("query", "rewrited_query", "query_merge_category_id as category_id","query_merge_is_game as is_game")
        //        val userQueryArray = orgDf
        //            .selectExpr("id", "query")
        //            .join(queryDf,Seq("query"))
        //            .selectExpr("id", "query","category_id","is_game")
        //            .rdd
        //            .map(row => (row.getString(0), (row.getString(1), row.getDouble(2), row.getDouble(3))))
        //            .groupByKey()
        //            .map(row => {
        //              val id = row._1
        //              val queryArray = new ArrayBuffer[String]()
        //              val otherCatArray = new ArrayBuffer[Double]()
        //              val appCatArray = new ArrayBuffer[Double]()
        //              val gameCatArray = new ArrayBuffer[Double]()
        //              row._2.toArray.foreach(s => {
        //                val (queryContent,categoryId, isGame) = s
        //                isGame.toInt match {
        //                  case 0 => otherCatArray.append(categoryId)
        //                  case 1 => gameCatArray.append(categoryId)
        //                  case 2 => appCatArray.append(categoryId)
        //                  case _ =>
        //                }
        //                queryArray.append(queryContent)
        //              })
        //              (id, queryArray.distinct,appCatArray.distinct, gameCatArray.distinct,otherCatArray.distinct)
        //            })
        //            .toDF("id", "query","query_app_id", "query_game_id","query_other_id")


        val userSearchActionArray = orgDf
            .selectExpr("id", "package_name", "action_type")
            .rdd
            .map(row => (row.getString(0), (row.getString(1), row.getString(2))))
            .groupByKey()
            .map(row => {
                val id = row._1
                val downloadAppArray = new ArrayBuffer[String]()
                val clickAppArray = new ArrayBuffer[String]()
                row._2.toArray.foreach(s => {
                    val (packageName, behaviorType) = s
                    behaviorType match {
                        case "download_install" => downloadAppArray.append(packageName)
                        case "item_click" => clickAppArray.append(packageName)
                    }
                })
                (id, downloadAppArray.distinct, clickAppArray.distinct)
            })
            .toDF("id", "download_pkg", "click_pkg")


        val result = userSearchActionArray
        //            .join(userQueryArray,Seq("id"),"outer")
        result.write.mode("overwrite").save(s"$outputPath/date=$endDate")


    }

}
