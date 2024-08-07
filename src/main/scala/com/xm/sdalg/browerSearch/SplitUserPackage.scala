package com.xm.sdalg.browerSearch

import com.xm.sdalg.browerSearch.Utils.getWhiteUserDf
import com.xm.sdalg.commons.SparkContextUtils.createSparkSession
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

/**
 * n等分人群包，并附加白名单
 */
object SplitUserPackage {
    def main(args: Array[String]): Unit = {
        println("输入的参数为：", args.mkString("Array(", ", ", ")"))
        splitUserPackage(args)
    }

    def splitUserPackage(args: Array[String]): Unit = {
        val sparkSession: SparkSession = createSparkSession(this.getClass.getSimpleName)
        val Array(nStr: String, date: String) = args.slice(0, 2)
        val packageName: Array[String] = args.slice(2, args.length)
        val whiteUserDf: DataFrame = getWhiteUserDf(sparkSession).cache()
        val n = nStr.toInt
        val weights = Array.fill(n)(1.0)
        val m = packageName.length
        for (k <- 0.until(m)) {
            val hdfspath = s"/user/h_data_platform/platform/browser/dwd_browser_push_user_package/packagename=${packageName(k)}/date=${date}"
            val df = sparkSession.read.text(hdfspath)
            val splitDFs = df.randomSplit(weights)

            for (i <- 0 until n) {
                val hdfspath_i: String = s"/user/h_data_platform/platform/browser/dwd_browser_push_user_package/packagename=${packageName(k)}-${i + 1}/date=${date}"
                alterTableInAlpha(sparkSession, splitDFs(i), whiteUserDf, hdfspath_i, packageName(k), i, date)
            }
        }
    }

    //将csv数据存储到hdfs路径下，并做映射
    def alterTableInAlpha(sparkSession: SparkSession,
                          splitDFs_i: Dataset[Row], whiteUserDf: DataFrame,
                          hdfspath_i: String, packageName: String,
                          i: Int, date: String): Unit = {
        splitDFs_i.union(whiteUserDf).distinct()
          .repartition(2).write.mode("overwrite").csv(hdfspath_i)
        sparkSession.sql(
            s"""
               |alter table hive_zjyprc_hadoop.browser.dwd_browser_push_user_package add if not exists partition(packagename='${packageName}-${i + 1}',date='${date}')
               |""".stripMargin)
    }
}
