package com.xm.sdalg.appstrore_recommend

import com.xm.sdalg.appstrore_recommend.Utils._
import com.xm.sdalg.commons.ClusterUtils.locale2locale
import com.xm.sdalg.commons.CollaborativeFilteringUtils.metric4res
import com.xm.sdalg.commons.HDFSUtils._
import com.xm.sdalg.commons.SparkContextUtils.createSparkSession
import com.xm.sdalg.commons.TimeUtils.getCurrentDateStr
import org.apache.spark.sql.SparkSession

object MultimodI2iMap {
    def main(args: Array[String]): Unit = {
        val Array(locale: String, endDate: String, emb: String) = args
        val sparkSession: SparkSession = createSparkSession(this.getClass.getSimpleName)
        import sparkSession.implicits._
        val allAppFilterSAB_CDV7 = getAppSetBc(sparkSession, locale, endDate, "SAB", "CD")
        val orgDf = sparkSession.read.text(s"/user/h_data_platform/platform/appstore/dim_appstore_faiss/tag=mulitmod_${emb}/date=${endDate}/res.csv")
            .rdd
            .map(_.getString(0).split(","))
            .map(row => (row(0), (row(2), row(1).toDouble)))
            .groupByKey()
            .map(row => (row._1, row._2.toArray.sortBy(_._1).slice(0, 100).toMap))
            .toDF("id", s"mulitmod_${emb}_org")
        val embDf = sparkSession.read.text(s"/user/h_data_platform/platform/appstore/dim_appstore_faiss/tag=mulitmod_${emb}/date=${endDate}/res.csv")
            .rdd
            .map(_.getString(0).split(","))
            .map(row => (row(0), (row(2), 100 - row(1).toDouble)))
            .filter(row => row._1 != row._2._1)
            .filter(row => allAppFilterSAB_CDV7.value.contains(row._2._1))
            .groupByKey()
            .filter(_._2.toArray.length > 0)
            .map(row => (row._1, row._2.toArray.sortBy(_._1).reverse.slice(0, 100).toMap))
            .toDF("id", s"mulitmod_${emb}")
        val truncateDf = sparkSession.read.text(s"/user/h_data_platform/platform/appstore/dim_appstore_faiss/tag=mulitmod_${emb}/date=${endDate}/res.csv")
            .rdd
            .map(_.getString(0).split(","))
            .map(row => (row(0), (row(2), 100 - row(1).toDouble)))
            .filter(row => row._2._2 >= (100 - 0.42))
            .filter(row => row._1 != row._2._1)
            .filter(row => allAppFilterSAB_CDV7.value.contains(row._2._1))
            .groupByKey()
            .filter(_._2.toArray.length > 0)
            .map(row => (row._1, row._2.toArray.sortBy(_._1).reverse.slice(0, 100).toMap))
            .toDF("id", s"mulitmod_${emb}_truncate")

        val resultDf = truncateDf

        metric4res(resultDf, "id", s"mulitmod_${emb}_truncate")
        resultDf.show(false)
        resultDf.printSchema()

        resultDf.write.mode("overwrite").save(s"$dwd_appstore_recommend_swing_i2i_1d/locale=${locale2locale(locale)}/tag=multimodi2i_${emb}/version=V1/date=$endDate")
    }
}
