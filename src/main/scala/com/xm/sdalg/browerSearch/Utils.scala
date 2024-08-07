package com.xm.sdalg.browerSearch

import com.xm.growth.push.redis.RedisClient
import com.xm.sdalg.commons.MysqlUtils.{abtestAbdataTable, abtestColumn, abtestDataBase, abtestPassword, abtestUser, c4_mig3_sdalg_stage00, mysqlPort, writeToMysql}
import com.xm.sdalg.commons.TimeUtils.getCurrentTimestamp
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

object Utils {
    // 用户特征表地址
    val dwd_browser_search_user_feature = "/user/h_data_platform/platform/browser/dwd_browser_search_user_feature"
    // 浏览器用户特征
    val dwd_br_user_online_fea = "/user/h_data_platform/platform/browser/dwd_br_user_online_fea"
    // 离线物料打分
    val offlineScoreSet: Array[String] = Array("position_score", "position_updatehour_score", "ctr_score")


    def getWhiteUserDf(sparkSession: SparkSession) = {
        sparkSession.read.text("/user/h_data_platform/platform/browser/dwd_browser_push_user_package/packagename=browser-search-whiteuser/whiteuser.txt")
    }

    /**
     * 实验报告数据生成及写入mysql
     */
    def getAndWritExpIndex(sparkSession: SparkSession,
                           dfwithExpid: DataFrame): Unit = {
        dfwithExpid.createOrReplaceTempView("temp")
        val sqlout = sparkSession.sql(
            s"""
               |with stat as (
               |        select
               |            expid,
               |            push_date,
               |            count(
               |                case
               |                    when activetype='EXPOSE' THEN 1
               |                    ELSE null
               |                END
               |            ) as pv_expose,
               |            count(
               |                case
               |                    when activetype='CLICK' THEN 1
               |                    ELSE null
               |                END
               |            ) as pv_click,
               |            count(
               |                distinct case
               |                    when activetype='EXPOSE' THEN userid
               |                    ELSE null
               |                END
               |            ) as uv_expose,
               |            count(
               |                distinct case
               |                    when activetype='CLICK' THEN userid
               |                    ELSE null
               |                END
               |            ) as uv_click
               |        from
               |            temp
               |        group by
               |            push_date,
               |            expid)
               |SELECT
               |    push_date,
               |    expid,
               |    pv_expose,
               |    pv_click,
               |    pv_click/pv_expose as pv_ctr,
               |    uv_expose,
               |    uv_click,
               |    uv_click/uv_expose as uv_ctr
               |from
               |    stat
               |ORDER BY
               |    push_date,
               |    expid
               |""".stripMargin)
            .selectExpr("push_date", "expid", "pv_expose", "pv_click", "pv_ctr", "uv_expose", "uv_click", "uv_ctr")
        val resultRdd = sqlout
            .rdd
            .flatMap(r => {
                val expid = r.getAs[String]("expid")
                val push_date = r.getAs[String]("push_date")
                val pv_expose = r.getAs[Int]("pv_expose").toString
                val pv_click = r.getAs[Int]("pv_click").toString
                val pv_ctr = r.getAs[Double]("pv_ctr").toString
                val uv_expose = r.getAs[Int]("uv_expose").toString
                val uv_click = r.getAs[Int]("uv_click").toString
                val uv_ctr = r.getAs[Double]("uv_ctr").toString

                val currentDateAndTime = getCurrentTimestamp
                var res: Array[(String, String, String, String, String)] = Array(
                    (push_date, expid, s"pv_expose", pv_expose, currentDateAndTime),
                    (push_date, expid, s"pv_click", pv_click, currentDateAndTime),
                    (push_date, expid, s"pv_ctr", pv_ctr, currentDateAndTime),
                    (push_date, expid, s"uv_expose", uv_expose, currentDateAndTime),
                    (push_date, expid, s"uv_click", uv_click, currentDateAndTime),
                    (push_date, expid, s"uv_ctr", uv_ctr, currentDateAndTime),
                )
                res
            })
            .map(row => s"${row._1}\t${row._2}\t${row._3}\t${row._4}\t${row._5}")
        resultRdd.take(100000).foreach(println)
        writeToMysql(
            sparkSession,
            resultRdd.map(_.split("\t")).map(row => Array(row(0), row(1), row(2), row(3), row(4))),
            abtestColumn, c4_mig3_sdalg_stage00, mysqlPort, abtestDataBase, abtestAbdataTable, abtestUser, abtestPassword)
    }
}
