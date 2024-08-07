package com.xm.sdalg.appstrore_recommend

import com.xm.sdalg.appstrore_recommend.Utils._
import com.xm.sdalg.commons.CalcUtils._
import com.xm.sdalg.commons.ClusterUtils._
import com.xm.sdalg.commons.HDFSUtils._
import com.xm.sdalg.commons.SparkContextUtils.createSparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions._

import scala.collection.mutable.Seq

/**
 * @zhaoliang6 on 20220217
 *             用户喜欢的物料类别
 */
object UserHotCate {
    def main(args: Array[String]): Unit = {
        val Array(locale: String, startDate: String, endDate: String) = args
        val sparkSession: SparkSession = createSparkSession(this.getClass.getSimpleName)
        val userCtrRdd: RDD[(String, (String, Double))] = getUserAppCtr(sparkSession, startDate, endDate, locale).cache()
        val userHotCateV1 = hotCate(sparkSession, userCtrRdd).withColumnRenamed("apps", "userHotCateContentOverSea")

        val userInstallRdd = getUserHotCateByInstallPv(sparkSession, startDate, endDate, locale)
        val userHotCateV2 = hotCate(sparkSession, userInstallRdd).withColumnRenamed("apps", "userHotCateByInstall")

        val resultDf = userHotCateV1.join(userHotCateV2, Seq("id"), "outer")
        resultDf.show(false)
        resultDf.printSchema()
        resultDf.write.mode("overwrite").save(s"/user/h_data_platform/platform/appstore/dwd_appstore_userhotcat_1d/locale=${locale2locale(locale)}/date=$endDate")
        alterTableIPartitionInAlpha(sparkSession, locale2catalog(locale, "hive"), "appstore", "dwd_appstore_userhotcat_1d", Map("date" -> endDate))
    }

    def getUserHotCateByInstallPv(sparkSession: SparkSession, startDate: String, endDate: String, locale: String): RDD[(String, (String, Double))] = {
        readParquet4dwd_app_stat_event_di(sparkSession, startDate, endDate, locale)
            .where(s"event_type in ('install')")
            .where(s"user_id = 0")
            .where(filterLocale("region", locale))
            .join(package2intlCategoryId(sparkSession, locale).selectExpr("package_name", "intl_category_id as intlCategoryId"), Seq("package_name"))
            .selectExpr("imeimd5 as gaid", "package_name", "intlCategoryId", "date")
            .groupBy("gaid", "intlCategoryId").count()
            .rdd
            .map(row => {
                (row.getAs[String]("gaid"), (row.getAs[Long]("intlCategoryId").toString, row.getAs[Long]("count").toDouble))
            })
            .filter(r => r._2._2 > 0)
    }

    /**
     * (gaid，(category,ctr))
     */
    def getUserAppCtr(sparkSession: SparkSession, startDate: String, endDate: String, locale: String): RDD[(String, (String, Double))] = {
        val package2idDf = package2intlCategoryId(sparkSession, locale)
        val rdd: RDD[(String, (String, Double))] = readParquet4dwd_appstore_client_track(sparkSession, startDate, endDate, locale)
            .where("gaid is not null and gaid != '' and package_name is not null and package_name != ''")
            .where("action_type in ('item_exposure','item_click')")
            .selectExpr("gaid", "package_name as packageName", "action_type as behaviorType", "date")
            .distinct()
            .groupBy("gaid", "packageName", "behaviorType").count()
            .join(package2idDf.selectExpr("package_name as packageName", "intl_category_id as intlCategoryId"), Seq("packageName"))
            .where(s"intlCategoryId is not null")
            .groupBy("gaid", "intlCategoryId").agg(collect_list(struct("behaviorType", "count")).as("struct"))
            .rdd
            .map(r => {
                val struct = r.getAs[collection.mutable.WrappedArray[GenericRowWithSchema]](s"struct")
                var EXPOSURE = 1
                var DOWNLOAD_COMPLETE = 0
                struct.foreach(s => {
                    val behaviorType = s.getAs[String]("behaviorType")
                    val count = s.getAs[Long]("count").toInt
                    behaviorType match {
                        case "item_exposure" => EXPOSURE += count
                        case "item_click" => DOWNLOAD_COMPLETE += count
                    }
                })
                val ctr = wilsonCtrUDF(DOWNLOAD_COMPLETE, EXPOSURE)
                (r.getAs[String]("gaid"),
                    (r.getAs[Long]("intlCategoryId").toString, ctr))
            })
            .filter(r => r._2._2 > 0)
        rdd
    }

    /**
     * 根据id区分游戏和应用，分别找到热门的游戏类别和应用类别
     *
     * @param rdd (id,(cate,ctr))
     */
    def hotCate(sparkSession: SparkSession, rdd: RDD[(String, (String, Double))]): DataFrame = {
        import sparkSession.implicits._
        val hotGame = group(rdd.filter(_._2._1.toInt > 100))
        val hotApp = group(rdd.filter(_._2._1.toInt < 100))
        hotGame.union(hotApp)
            .map(r => (r._1, (r._2.toString, r._3)))
            .groupByKey()
            .map(r => (r._1, r._2.toMap))
            .toDF("id", "apps")
    }

    /**
     * 将输入的rdd,找到当前id下的ctr前10的类别
     *
     * @param rdd (id,(cate,ctr))
     */
    def group(rdd: RDD[(String, (String, Double))]): RDD[(String, String, Double)] = {
        rdd
            .groupByKey()
            .flatMap(r => r._2.toArray.sortBy(_._2).reverse.slice(0, 10).map(ii => (r._1, ii._1, ii._2)))
    }

}
