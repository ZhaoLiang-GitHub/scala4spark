package com.xm.sdalg.appstrore_recommend

import com.xm.sdalg.appstrore_recommend.Utils.getAppSetBc
import com.xm.sdalg.commons.HDFSUtils._
import com.xm.sdalg.commons.SparkContextUtils.createSparkSession
import com.xm.sdalg.commons.TimeUtils.getCurrentDateStr
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

object MultimodI2iDownload {
    def main(args: Array[String]): Unit = {
        val Array(locale: String, endDate: String, emb: String) = args
        val sparkSession: SparkSession = createSparkSession(this.getClass.getSimpleName)
        import sparkSession.implicits._
        val allAppFilterSAB_CDV7: Broadcast[Set[String]] =
            if (locale == "ID")
                getAppSetBc(sparkSession, locale, getCurrentDateStr(), "SAB", "CD")
            else
                getAppSetBc(sparkSession, locale, getCurrentDateStr(), "SAB", "C")

        val res = sparkSession.read.parquet(getLatestExistPath(sparkSession, "/user/h_data_platform/platform/appstore/app_embeddings_df/date=yyyyMMdd", endDate, 365))
            .selectExpr("pkg", emb)
            .filter(row => allAppFilterSAB_CDV7.value.contains(row.getString(0)))
            .rdd
            .map(row => (row.getString(0), row.getSeq[Double](1).toArray))
            .filter(_._2.length == 512)
            .mapValues(_.mkString(" "))
            .toDF("pkg", emb)

        res.show(false)
        res.printSchema()
        println(res.count())
        dfSaveInAssignPath(sparkSession, s"/user/h_data_platform/platform/appstore/dim_appstore_faiss/tag=mulitmod_${emb}/date=${endDate}/vec.csv", res, "csv")
    }
}
