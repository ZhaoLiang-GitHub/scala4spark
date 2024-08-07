package com.xm.sdalg.appstrore_recommend

import com.xm.sdalg.appstrore_recommend.Utils._
import com.xm.sdalg.commons.HDFSUtils._
import com.xm.sdalg.commons.SparkContextUtils.createSparkSession
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.xm.sdalg.commons.ClusterUtils._

/**
 * @author zhaoliang6 on 20220808
 *         海外商店推荐 用户在GA内曝光过的item做过滤
 */
object UserExposure {
    def main(args: Array[String]): Unit = {
        val Array(locale: String, startDate: String, endDate: String) = args
        val sparkSession: SparkSession = createSparkSession(this.getClass.getSimpleName)
        import sparkSession.implicits._
        val orgDf = readParquet4dwm_appstore_user_behavior_di(sparkSession, locale, startDate, endDate)
            .where(filterLocale("locale", locale))
            .where("id.id is not null and packageName is not null")
            .where("behaviorType in ('EXPOSURE')")
            .selectExpr("id.id as id", "packageName")
            .groupBy("id", "packageName").count()
            .cache()
        var result: DataFrame = null
        Array(1, 2, 6, 8).foreach(count => {
            val temp = orgDf
                .where(s"count >= $count")
                .rdd
                .map(row => (row.getAs[String]("id"), row.getAs[String]("packageName")))
                .groupByKey()
                .map(row => {
                    (row._1, row._2.toSet.toArray.map(ii => (ii, count)).toMap)
                })
                .toDF("id", s"expAppSet$count")
            if (result == null) result = temp
            else result = result.join(temp, Seq("id"), "outer")
        })
        result = result
            .where("id is not null")
        result.show(false)
        result.printSchema()
        result.write.mode("overwrite").parquet(s"/user/h_data_platform/platform/appstore/dwd_appstore_recommend_user_expouser_1d/locale=${locale2locale(locale)}/tag=parquet/date=$endDate")
    }
}
