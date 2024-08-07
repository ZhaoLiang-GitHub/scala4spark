package com.xm.sdalg.appstrore_recommend

import com.xm.sdalg.appstrore_recommend.Utils._
import com.xm.sdalg.commons.ClusterUtils._
import com.xm.sdalg.commons.HDFSUtils.alterTableIPartitionInAlpha
import com.xm.sdalg.commons.SparkContextUtils.createSparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


/**
 * @zhaoliang6 on 20220217
 *             用户喜欢的物料类别
 */
object UserHotCateByNewGp {
    def main(args: Array[String]): Unit = {
        val Array(locale: String, startDate: String, endDate: String, topN: String) = args
        val sparkSession: SparkSession = createSparkSession(this.getClass.getSimpleName)
        import sparkSession.implicits._
        val package2intlCategoryIdDf = package2intlCategoryId(sparkSession, locale)
            .selectExpr("package_name as packageName", "intl_category_id as intlcategoryid")
        var orgDf = readParquet4dwm_appstore_user_behavior_di(sparkSession, locale, startDate, endDate)
            .where(filterLocale("locale", locale))
            .where(filterGaid("id.id"))
            .where("id.id is not null and packageName is not null")
            .where("behaviorType in ('ACTIVE','INSTALL_COMPLETE','DOWNLOAD','DETAIL','EXPOSURE') ")
            .selectExpr("id.id as id", "packageName", "behaviorType", "date")
        val whiteUser = orgDf
            .where("behaviorType in ('DOWNLOAD_COMPLETE','DOWNLOAD') ")
            .groupBy("id", "date").agg(countDistinct("packageName").as("count"))
            .where("count <= 200")
            .selectExpr("id")
        orgDf = orgDf
            .join(whiteUser, Seq("id"))
            .join(package2intlCategoryIdDf, Seq("packageName"))

        val userScoreByNewGp = orgDf
            .groupBy("id", "intlcategoryid", "behaviorType").agg(count("packageName").as("score"))
            .selectExpr("id", "intlcategoryid", "behaviorType", "score")
            .rdd
            .map(row => {
                ((row.getString(0), row.getInt(1)), (row.getString(2), row.getLong(3)))
            })
            .groupByKey()
            .map(row => {
                val temp: Map[String, Int] = row._2.toMap.mapValues(_.toInt)
                val score = (temp.getOrElse("DETAIL", 0) + temp.getOrElse("DOWNLOAD_COMPLETE", 0) + temp.getOrElse("DOWNLOAD", 0)) / 4.0 + temp.getOrElse("INSTALL_COMPLETE", 0) / 2.0
                (row._1._1, (row._1._2, score))
            })
            .groupByKey()
            .map(row => (row._1, row._2.toArray.sortBy(_._2).reverse.slice(0, topN.toInt).map(_._1).toSet))
            .toDF("id", "userCateSortByNewGp")
        userScoreByNewGp.show(false)
        userScoreByNewGp.printSchema()
        userScoreByNewGp.write.mode("overwrite").parquet(s"$dwd_appstore_recommend_user_offline_behavior_1d/locale=${locale2locale(locale)}/tag=userHotCateByNewGp/date=$endDate")
        alterTableIPartitionInAlpha(sparkSession, locale2catalog(locale, "hive"), "appstore", "dwd_appstore_recommend_user_offline_behavior_1d", Map("date" -> endDate, "locale" -> locale2locale(locale), "tag" -> "userHotCateByNewGp"))

    }


}
