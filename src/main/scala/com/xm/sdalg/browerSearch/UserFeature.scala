package com.xm.sdalg.browerSearch

import com.xm.data.browser.fea.user_onLine_fea
import com.xm.data.browser.searchPush.DwdBrowserSearchUserFeature
import com.xm.data.commons.spark.HdfsIO.{RDDThriftOutputWrapper, SparkContextThriftFileWrapper}
import com.xm.sdalg.browerSearch.Utils._
import com.xm.sdalg.commons.HDFSUtils._
import com.xm.sdalg.commons.SparkContextUtils._
import com.xm.sdalg.commons.TimeUtils._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.lit

import java.lang
import scala.collection.JavaConverters._
import scala.collection.mutable

object UserFeature {
    val currentDate: String = getCurrentDateStr()
    val zero_256 = List.fill(256)(0.0).map(Double.box)

    def main(args: Array[String]): Unit = {
        val sparkSession: SparkSession = createSparkSession(this.getClass.getSimpleName)
        val browserUserFeaturePath = getLatestExistPath(sparkSession, s"${dwd_br_user_online_fea}/date=yyyyMMdd", currentDate)
        val browserUserFeatureDf = sparkSession.sparkContext.thriftSeqDataFrame(browserUserFeaturePath, classOf[user_onLine_fea])
        val userPackageDf = getUserPackageType(sparkSession)


        val resDf = userPackageDf
            .join(browserUserFeatureDf, Seq("userid"), "full")
            .cache()
        val schema = resDf.schema.fieldNames
        val schema2index = Range(0, schema.length).map(i => (schema(i), i)).toMap
        val resRdd = userPackageDf
            .join(browserUserFeatureDf, Seq("userid"), "full")
            .dropDuplicates("userid")
            .rdd
            .map(row => {
                val obj = new DwdBrowserSearchUserFeature()

                obj.setUserid(row.getAs[String]("userid"))
                obj.setUser_sex(row.getAs[Double]("user_sex"))
                obj.setUser_age_6_level(row.getAs[Double]("user_age_6_level"))
                obj.setUser_age_8_level(row.getAs[Double]("user_age_8_level"))
                obj.setDegree_3_level(row.getAs[Double]("degree_3_level"))
                obj.setPhone_brand(row.getAs[Double]("phone_brand"))
                obj.setPhone_series(row.getAs[Double]("phone_series"))
                obj.setCurr_city_type(row.getAs[Double]("curr_city_type"))
                obj.setPhone_retail_price(row.getAs[Double]("phone_retail_price"))
                obj.setOn_office(row.getAs[Double]("on_office"))
                obj.setIs_commuter(row.getAs[Double]("is_commuter"))
                obj.setInterest_finance_level(row.getAs[Double]("interest_finance_level"))
                obj.setInterest_ecom_level(row.getAs[Double]("interest_ecom_level"))
                obj.setInterest_game_level(row.getAs[Double]("interest_game_level"))
                obj.setInterest_video_level(row.getAs[Double]("interest_video_level"))
                obj.setInterest_music_level(row.getAs[Double]("interest_music_level"))
                obj.setInterest_read_level(row.getAs[Double]("interest_read_level"))
                obj.setInterest_travel_level(row.getAs[Double]("interest_travel_level"))
                obj.setInterest_digitaltechnology_level(row.getAs[Double]("interest_digitaltechnology_level"))
                obj.setInterest_education_level(row.getAs[Double]("interest_education_level"))
                obj.setPhone_usage_30d_cnt(row.getAs[Double]("phone_usage_30d_cnt"))
                obj.setAvg_usage_30d(row.getAs[Double]("avg_usage_30d"))
                obj.setHas_car(row.getAs[Double]("has_car"))
                obj.setHas_house(row.getAs[Double]("has_house"))
                obj.setHas_child(row.getAs[Double]("has_child"))
                obj.setBr_push_click_pv_day(row.getAs[Double]("br_push_click_pv_day"))
                obj.setBr_push_expose_pv_day(row.getAs[Double]("br_push_expose_pv_day"))
                obj.setBr_push_click_pv_week(row.getAs[Double]("br_push_click_pv_week"))
                obj.setBr_push_expose_pv_week(row.getAs[Double]("br_push_expose_pv_week"))
                obj.setBr_push_click_pv_month(row.getAs[Double]("br_push_click_pv_month"))
                obj.setBr_push_expose_pv_month(row.getAs[Double]("br_push_expose_pv_month"))
                obj.setBr_feeds_active_days_month(row.getAs[Double]("br_feeds_active_days_month"))
                obj.setBr_feeds_active_days_week(row.getAs[Double]("br_feeds_active_days_week"))
                obj.setBr_feeds_active_day(row.getAs[Double]("br_feeds_active_day"))
                obj.setBr_feeds_active_days_month_icon(row.getAs[Double]("br_feeds_active_days_month_icon"))
                obj.setBr_feeds_active_days_week_icon(row.getAs[Double]("br_feeds_active_days_week_icon"))
                obj.setBr_feeds_active_day_icon(row.getAs[Double]("br_feeds_active_day_icon"))
                obj.setBr_feeds_active_days_month_push(row.getAs[Double]("br_feeds_active_days_month_push"))
                obj.setBr_feeds_active_days_week_push(row.getAs[Double]("br_feeds_active_days_week_push"))
                obj.setBr_feeds_active_day_push(row.getAs[Double]("br_feeds_active_day_push"))
                obj.setBr_feeds_active_days_month_other(row.getAs[Double]("br_feeds_active_days_month_other"))
                obj.setBr_feeds_active_days_week_other(row.getAs[Double]("br_feeds_active_days_week_other"))
                obj.setBr_feeds_active_day_other(row.getAs[Double]("br_feeds_active_day_other"))
                obj.setBr_app_active_day(row.getAs[Double]("br_app_active_day"))
                obj.setBr_app_active_days_week(row.getAs[Double]("br_app_active_days_week"))
                obj.setBr_app_active_days_month(row.getAs[Double]("br_app_active_days_month"))
                obj.setBr_push_click_days_in1week(row.getAs[Double]("br_push_click_days_in1week"))
                obj.setBr_push_click_days_in1month(row.getAs[Double]("br_push_click_days_in1month"))
                obj.setNh_push_click_pv_day(row.getAs[Double]("nh_push_click_pv_day"))
                obj.setNh_push_expose_pv_day(row.getAs[Double]("nh_push_expose_pv_day"))
                obj.setBr_cate_cnt(row.getAs[Double]("br_cate_cnt"))
                obj.setBr_cate_1(row.getAs[Double]("br_cate_1"))
                obj.setBr_cate_2(row.getAs[Double]("br_cate_2"))
                obj.setBr_cate_3(row.getAs[Double]("br_cate_3"))
                obj.setBr_cate_4(row.getAs[Double]("br_cate_4"))
                obj.setBr_cate_5(row.getAs[Double]("br_cate_5"))
                obj.setNh_push_click_pv_week(row.getAs[Double]("nh_push_click_pv_week"))
                obj.setNh_push_expose_pv_week(row.getAs[Double]("nh_push_expose_pv_week"))
                obj.setNh_push_click_pv_month(row.getAs[Double]("nh_push_click_pv_month"))
                obj.setNh_push_expose_pv_month(row.getAs[Double]("nh_push_expose_pv_month"))
                obj.setUser_package_type(row.getAs("user_package_type"))

                val user_emb = row.getAs[mutable.WrappedArray[lang.Double]]("user_emb")
                val nh_user_emb = row.getAs[mutable.WrappedArray[lang.Double]]("nh_user_emb")
                val user_emb_list = (if (user_emb != null) user_emb else zero_256).asJava
                val nh_user_emb_list = (if (nh_user_emb != null) nh_user_emb else zero_256).asJava
                obj.setUser_emb(user_emb_list)
                obj.setNh_user_emb(nh_user_emb_list)

                obj
            })

        resRdd.take(10).foreach(println)
        println(resRdd.count())
        val hdfsPath = s"hdfs://zjyprc-hadoop${dwd_browser_search_user_feature}/date=$currentDate"
        delete(sparkSession.sparkContext, hdfsPath)
        resRdd.repartition(200).saveAsSequenceFile(hdfsPath)
        alterTableIPartitionInAlpha(sparkSession, "hive_zjyprc_hadoop", "browser", "dwd_browser_search_user_feature", Map("date" -> currentDate))
    }

    /**
     * 从下发人包中获得人包类型
     */
    def getUserPackageType(sparkSession: SparkSession) = {
        Array("browser-search-v3-",
            "browser-search-v6-",
            "browser-search-v7-",
            "browser-search-nosearchuser-")
            .map(packageType => {
                val package2hivePartition = Array.range(8, 24, 1)
                    .map(hour_of_day => s"'${packageType}${hour_of_day}30'")
                    .mkString(",")
                sparkSession.sql(
                    s"""
                       |select * from hive_zjyprc_hadoop.browser.dwd_browser_push_user_package
                       |where date = ${currentDate} and packagename in ( ${package2hivePartition} )
                       |""".stripMargin)
                    .withColumn("user_package_type", lit(packageType.split("-")(2)))
                    .selectExpr("userid", "user_package_type")
            })
            .reduce(_ union _)
    }
}
