package com.xm.sdalg.appstrore_recommend

import com.xm.sdalg.appstrore_recommend.Utils._
import com.xm.sdalg.commons.ClusterUtils._
import com.xm.sdalg.commons.HDFSUtils._
import com.xm.sdalg.commons.MysqlUtils._
import com.xm.sdalg.commons.SparkContextUtils._
import com.xm.sdalg.commons.TimeUtils._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import scala.util.Try

/**
 * @author zhaoliang6 on 20220126
 *         海外商店推荐 实验数据写入实验平台
 */
object ExpReport {
    val connector = "_"

    def main(args: Array[String]): Unit = {
        val Array(locale: String, date: String) = args.slice(0, 2)
        val combinations: Array[String] = args.slice(2, args.length).filter(_.contains(connector))

        val sparkSession: SparkSession = createSparkSession(this.getClass.getSimpleName)
        var resultRdd: RDD[String] = null
        Array(
            dwm_mipicks_recommend_di, // 非缓存实验数据 https://dt.mi.com/workspace/?channel=nav#/workspace/10784/tableDetail?catalog=hive_alsgprc_hadoop&dbName=dw&tableNameEn=dwm_mipicks_recommend_di&region=sg
            dwm_mipicks_recommend_dapan_di // 全局统计数据 https://dt.mi.com/workspace/#/workspace/10784/tableDetail?catalog=hive_alsgprc_hadoop&dbName=dw&tableNameEn=dwm_mipicks_recommend_dapan_di&region=sg
        ).foreach(path => {
            var tempDf: DataFrame = null
            tempDf = sparkSession.read.parquet(s"$path/date=$date")
                .withColumn("date", lit(date))
                .where("model_id is not null and model_id != ''")
                .where(filterLocale("lo", locale))
                .where(
                    """
                      |stat_type in (
                      |'ALL',
                      |'主动打开', '被动调起',
                      |'下滑用户','非下滑用户',
                      |'Light', 'Middle', 'Heavy',
                      |'游戏', '应用',
                      |'top1000','top1001-2000','top2001-3000','top3001-4000','top4001-5000','5000+',
                      |'应用_Heavy','游戏_Heavy','应用_Light','游戏_Middle','游戏_Light','应用_Middle',
                      |'主动打开_Heavy','被动调起_Heavy','主动打开_Middle','被动调起_Middle','主动打开_Light','被动调起_Light'
                      |)
                      |""".stripMargin)
                .where("page in ('首页','详情页','游戏页')")
                .withColumn("isConnector", lit(false))
            if (combinations.length > 0) {
                // 多个实验组合并在一起看实验报告数据
                val col: Array[String] = tempDf.drop("lo", "model_id", "stat_type", "date", "page", "isConnector").columns
                val colMap: Map[String, String] = col.map((_, "sum")).toMap
                combinations.foreach(c => {
                    val combinationDf: DataFrame = tempDf
                        .where(s"model_id in (${c.split(connector).map(ii => s"'$ii'").mkString(",")})")
                        .withColumn("model_id", lit(c))
                        .groupBy("lo", "model_id", "stat_type", "date", "page").agg(colMap)
                        .selectExpr(col.map(i => s"`sum($i)` as $i") ++ Array("lo", "model_id", "stat_type", "date", "page"): _*)
                        .withColumn("isConnector", lit(true))
                    tempDf = tempDf.unionByName(combinationDf)
                })
            }
            if (localeInCluster(locale, singapore)) {
                val col: Array[String] = tempDf.drop("lo", "model_id", "stat_type", "date", "page", "isConnector").columns
                val colMap: Map[String, String] = col.map((_, "sum")).toMap
                val combinationDf: DataFrame = tempDf
                    .where(filterLocale("lo", SG_OTHER.mkString(","))) // 新加坡集群其他国家
                    .withColumn("lo", lit("SG_OTHER"))
                    .groupBy("lo", "model_id", "stat_type", "date", "page").agg(colMap)
                    .selectExpr(col.map(i => s"`sum($i)` as $i") ++ Array("lo", "model_id", "stat_type", "date", "page"): _*)
                    .withColumn("isConnector", lit(true))
                tempDf = tempDf.unionByName(combinationDf)
            }

            val pattern = if (path == dwm_mipicks_recommend_dapan_di) "_all" else ""
            resultRdd = union(resultRdd, example2report(tempDf, pattern = pattern, path = path))
        })
        writeToMysql(
            sparkSession,
            resultRdd.map(_.split("\t")).map(row => Array(row(0), row(1), row(2), row(3), row(4))),
            abtestColumn, c4_mig3_sdalg_stage00, mysqlPort, abtestDataBase, abtestAbdataTable, abtestUser, abtestPassword)
        //        rddSaveInHDFS(sparkSession, resultRdd, s"$dim_appstore_recommend_exp_1d/locale=${locale2locale(locale)}/date=$date", 1)
    }


    def example2report(orgDf: DataFrame,
                       statType: String = "",
                       eidType: String = "",
                       pattern: String,
                       path: String): RDD[String] = {
        val result = orgDf
            .rdd
            .flatMap(r => {
                val lo = r.getAs[String]("lo")
                val page = r.getAs[String]("page")
                val market_v = Try(r.getAs[String]("market_v")).getOrElse("ALL")
                var eid = lo + "_" + r.getAs[String]("model_id").replaceAll("-", "_") + eidType
                if (market_v == "ALL")
                    eid = eid
                else
                    eid = eid + market_v
                var stat_type =
                    if (statType != "")
                        statType
                    else
                        r.getAs[String]("stat_type")
                if (Array("top1000", "top1001-2000", "top2001-3000", "top3001-4000", "top4001-5000", "5000+").contains(stat_type))
                    stat_type = stat_type
                        .replace("-", "_")
                        .replace("top", "Top")
                        .replace("+", "")
                stat_type = stat_type + pattern
                val date = r.getAs[String]("date")
                val expose_uv = r.getAs[Long]("expose_uv").toDouble
                val expose_pv = r.getAs[Long]("expose_pv").toDouble
                val click_uv = r.getAs[Long]("click_uv").toDouble
                val click_pv = r.getAs[Long]("click_pv").toDouble
                val download_num = r.getAs[Long]("download_num").toDouble
                val download_uv = r.getAs[Long]("download_uv").toDouble
                val download_puv = r.getAs[Long]("download_puv").toDouble
                val install_num = r.getAs[Long]("install_num").toDouble
                val install_uv = r.getAs[Long]("install_uv").toDouble
                val install_puv = r.getAs[Long]("install_puv").toDouble
                val active_num = r.getAs[Long]("active_num").toDouble
                val active_uv = r.getAs[Long]("active_uv").toDouble
                val active_puv = r.getAs[Long]("active_puv").toDouble
                val discover_get_pv = r.getAs[Long]("discover_get_pv").toDouble
                val discover_get_uv = r.getAs[Long]("discover_get_uv").toDouble
                val discover_get_puv = r.getAs[Long]("discover_get_puv").toDouble
                val discover_detail_expose_uv = r.getAs[Long]("discover_detail_expose_uv").toDouble
                val discover_detail_expose_pv = r.getAs[Long]("discover_detail_expose_pv").toDouble
                val discover_detail_download_uv = r.getAs[Long]("discover_detail_download_uv").toDouble
                val discover_detail_download_pv = r.getAs[Long]("discover_detail_download_pv").toDouble
                val discover_detail_download_puv = r.getAs[Long]("discover_detail_download_puv").toDouble
                var down_scroll_rate = r.getAs[Double]("down_scroll_rate")
                var slide_rate = r.getAs[Double]("slide_rate")
                var avg_stay_dura = r.getAs[Double]("avg_stay_dura")
                var retain_rate = r.getAs[Double]("retain_rate")
                val isConnector = r.getAs[Boolean]("isConnector")
                if (isConnector) {
                    // 多个实验组合并时，这些数据无法使用
                    down_scroll_rate = -1D
                    slide_rate = -1D
                    avg_stay_dura = -1D
                    retain_rate = -1D
                }
                val currentDateAndTime = getCurrentTimestamp
                var res: Array[(String, String, String, Double, String)] = Array(
                    (date, eid, s"${page}_${stat_type}_cvr_pv", download_puv / expose_pv, currentDateAndTime), // pv下载率
                    (date, eid, s"${page}_${stat_type}_cvr_uv", download_uv / expose_uv, currentDateAndTime), // uv下载率
                    (date, eid, s"${page}_${stat_type}_avg_expose_download", download_puv / expose_uv, currentDateAndTime), // uv价值，下载人包/曝光uv
                    (date, eid, s"${page}_${stat_type}_ctr_pv", click_pv / expose_pv, currentDateAndTime), // pv点击率
                    (date, eid, s"${page}_${stat_type}_ctr_uv", click_uv / expose_uv, currentDateAndTime), // uv点击率
                    (date, eid, s"${page}_${stat_type}_expose_pv", expose_pv, currentDateAndTime), // 组件曝光PV
                    (date, eid, s"${page}_${stat_type}_expose_uv", expose_uv, currentDateAndTime), // 组件曝光UV
                    (date, eid, s"${page}_${stat_type}_click_pv", click_pv, currentDateAndTime), // 点击PV
                    (date, eid, s"${page}_${stat_type}_click_uv", click_uv, currentDateAndTime), // 点击UV
                    (date, eid, s"${page}_${stat_type}_download_pv", download_num, currentDateAndTime), // 下载PV
                    (date, eid, s"${page}_${stat_type}_download_uv", download_uv, currentDateAndTime), // 下载UV
                    (date, eid, s"${page}_${stat_type}_download_puv", download_puv, currentDateAndTime), // 下载PUV
                    (date, eid, s"${page}_${stat_type}_download_pv_detail", discover_detail_download_pv, currentDateAndTime), // 详情页内下载PV
                    (date, eid, s"${page}_${stat_type}_download_uv_detail", discover_detail_download_uv, currentDateAndTime), // 详情页内下载UV
                    (date, eid, s"${page}_${stat_type}_download_puv_detail", discover_detail_download_puv, currentDateAndTime), // 详情页内下载PUV
                    (date, eid, s"${page}_${stat_type}_download_pv_get", discover_get_pv, currentDateAndTime), // get下载PV
                    (date, eid, s"${page}_${stat_type}_download_uv_get", discover_get_uv, currentDateAndTime), // get下载UV
                    (date, eid, s"${page}_${stat_type}_download_puv_get", discover_get_puv, currentDateAndTime), // get下载PUV
                    (date, eid, s"${page}_${stat_type}_down_scroll_rate", down_scroll_rate, currentDateAndTime), // 下滑率
                    (date, eid, s"${page}_${stat_type}_slide_rate", slide_rate, currentDateAndTime), // 卡片1横滑率
                    (date, eid, s"${page}_${stat_type}_retain_rate", retain_rate, currentDateAndTime), // 次留
                    (date, eid, s"${page}_${stat_type}_install_num", install_num, getCurrentTimestamp), // 安装数
                    (date, eid, s"${page}_${stat_type}_install_uv", install_uv, getCurrentTimestamp), // 安装uv
                    (date, eid, s"${page}_${stat_type}_install_puv", install_puv, getCurrentTimestamp), // 安装puv
                    (date, eid, s"${page}_${stat_type}_active_num", active_num, getCurrentTimestamp), // 激活数
                    (date, eid, s"${page}_${stat_type}_active_uv", active_uv, getCurrentTimestamp), // 激活uv
                    (date, eid, s"${page}_${stat_type}_active_puv", active_puv, getCurrentTimestamp), // 激活puv
                    (date, eid, s"${page}_${stat_type}_discover_detail_expose_uv", discover_detail_expose_uv, getCurrentTimestamp), // 详情页内曝光UV
                    (date, eid, s"${page}_${stat_type}_discover_detail_expose_pv", discover_detail_expose_pv, getCurrentTimestamp), // 详情页内曝光PV
                )
                if (path == dwm_mipicks_recommend_dapan_di) {
                    // 只在大盘表中有统计
                    val search_retain_rate: Double = Try {
                        r.getAs[Double]("search_retain_rate")
                    }.getOrElse {
                        Try {
                            r.getAs[Long]("search_retain_rate").toDouble
                        }.getOrElse(-1D)
                    }
                    val search_uv: Double = Try {
                        r.getAs[Long]("search_uv").toDouble
                    }.getOrElse(-1D)
                    val search_retain_uv: Double = Try {
                        r.getAs[Long]("search_retain_uv").toDouble
                    }.getOrElse(-1D)

                    res ++= Array(
                        (date, eid, s"${page}_${stat_type}_search_retain_rate", search_retain_rate, currentDateAndTime), // 搜索留存率
                        (date, eid, s"${page}_${stat_type}_search_uv", search_uv, currentDateAndTime), // 搜索uv
                        (date, eid, s"${page}_${stat_type}_search_retain_uv", search_retain_uv, currentDateAndTime), // 搜索留存uv
                    )
                }

                // 20231009 新增卸载率相关指标
                val uninstall_rate: Double = Try {
                    r.getAs[Double]("uninstall_rate").toDouble
                }.getOrElse(-1D)
                val eff_install_puv: Double = Try {
                    r.getAs[Long]("eff_install_puv").toDouble
                }.getOrElse(-1D)
                val adpdau: Double = Try {
                    r.getAs[Double]("adpdau").toDouble
                }.getOrElse(-1D)
                val uninstall_puv2 = Try {
                    r.getAs[Long]("uninstall_puv2").toDouble
                }.getOrElse(-1D)
                res ++= Array(
                    (date, eid, s"${page}_${stat_type}_uninstall_rate", uninstall_rate, currentDateAndTime), // 卸载率
                    (date, eid, s"${page}_${stat_type}_eff_install_puv", eff_install_puv, currentDateAndTime), // 有效安装puv
                    (date, eid, s"${page}_${stat_type}_adpdau", adpdau, currentDateAndTime), // adpdau
                    (date, eid, s"${page}_${stat_type}_uninstall_puv2", uninstall_puv2, currentDateAndTime), // 卸载puv
                )

                res
            })
            .filter(row => {
                val value = row._4
                !value.isNaN && !value.isInfinite
            })
            .map(row => s"${row._1}\t${row._2}\t${row._3}\t${row._4}\t${row._5}")
        result.distinct()
    }
}
