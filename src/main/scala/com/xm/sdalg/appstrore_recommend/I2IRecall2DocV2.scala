package com.xm.sdalg.appstrore_recommend

import com.xm.sdalg.appstrore_recommend.Utils._
import com.xm.sdalg.commons.HDFSUtils._
import com.xm.sdalg.commons.SparkContextUtils.createSparkSession
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.xm.sdalg.commons.ClusterUtils._

/**
 * i2i召回方式合并写入doc
 * 当有多种i2i方式线上使用时，需要将多种方式outerJoin在一起同时写入
 */
object I2IRecall2DocV2 {
    def main(args: Array[String]): Unit = {
        val Array(locale: String, date: String) = args.slice(0, 2)
        val pathArray = args.slice(2, args.length)
        val sparkSession: SparkSession = createSparkSession(this.getClass.getSimpleName)
        var resultDf: DataFrame = null
        // 明确生成的表直接join
        pathArray.foreach(path => {
            val temp = sparkSession.read.parquet(getLatestExistPath(sparkSession, orgPath = s"$path/date=yyyyMMdd", date,365))
                .dropDuplicates("id")
            temp.show(false)
            temp.printSchema()
            if (resultDf == null) resultDf = temp
            else resultDf = resultDf.join(temp, Seq("id"), "outer")
        })

        resultDf.show(false)
        resultDf.printSchema()
        resultDf.write.mode("overwrite").parquet(dwd_appstore_recommend_i2i_1d(date, locale2locale(locale) + "parquet"))
        rddSaveInHDFS(sparkSession, resultDf.toJSON.rdd, dwd_appstore_recommend_i2i_1d(date, locale2locale(locale)), 1000)
        alterTableIPartitionInAlpha(sparkSession, locale2catalog(locale, "hive"), "appstore", "dwd_appstore_recommend_i2i_1d", Map("date" -> date))

    }
}