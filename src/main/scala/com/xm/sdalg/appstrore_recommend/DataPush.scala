package com.xm.sdalg.appstrore_recommend

import com.xm.sdalg.appstrore_recommend.Utils._
import com.xm.sdalg.commons.SparkContextUtils._
import com.xm.sdalg.commons.HDFSUtils._
import com.xm.sdalg.commons.EmailUtils._
import com.xm.sdalg.commons.TimeUtils._
import org.apache.commons.mail.EmailAttachment
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import com.xm.sdalg.commons.ClusterUtils._
import org.apache.spark.sql.functions.countDistinct

/**
 * 数据推送作业
 */
object DataPush {
    def main(args: Array[String]): Unit = {
        val Array(locale: String, endDate: String) = args
        val sparkSession: SparkSession = createSparkSession(this.getClass.getSimpleName)
        import sparkSession.implicits._
        val date = getLastIcebergDate(sparkSession, s"${locale2catalog(locale, "iceberg")}.appstore.dwd_ias_recommend_app_di", endDate)
        println("当前读取dwd_ias_recommend_app_di的日期为：" + date)
        val v3  = sparkSession.table(s"${locale2catalog(locale, "iceberg")}.appstore.dwd_ias_recommend_app_di")
            .where(s"date = '${date}'")
            .where(filterLocale("locale", locale))
            .selectExpr("packagename", "level")
            .groupBy("level").count()
            .rdd
            .map(row => (row.getAs[String]("level"), row.getAs[Long]("count")))
            .collect()
            .mkString(" ")
        if (locale == "ID") {
            // 只在印尼跑一份全球数据
            val write = sparkSession.table(s"${locale2catalog(locale, "iceberg")}.appstore.dwd_ias_recommend_app_di")
                .where(s"date = '${date}'")
                .selectExpr("packagename", "locale", "level")
                .rdd
                .map(row => (row.getString(0), (row.getString(1), row.getString(2))))
                .groupByKey()
                .map(row => s"${row._1}\t${row._2.map(i => i._1 + ":" + i._2).mkString(",")}")
                .toDF("value")
            println(s"全球数据一共 ${write.count()} 条")
            dfSaveInAssignPath(sparkSession, "/user/h_data_platform/platform/appstore/dim_appstore_recommend_app_level_1d/data/app_level.txt", write, "txt")

            // 核心国家增加数据报警
            val all = sparkSession.table(s"${locale2catalog(locale, "iceberg")}.appstore.dwd_ias_recommend_app_di")
                .where(s"date between ${getFrontDay(date, 30)} and $date")
                .where(filterLocale("locale", "ID"))
                .where("level in ('S','A', 'B', 'C', 'D')")
                .groupBy("date").agg(countDistinct("packagename").as("count"))
            val thisDayAll: Long = all
                .where(s"date='${date}'")
                .selectExpr("count")
                .rdd
                .map(_.getLong(0))
                .collect().head
            val frontDayAll: Long = all
                .where(s"date between '${getFrontDay(date, 30)}' and '${getFrontDay(date, 1)}'")
                .selectExpr("count")
                .rdd
                .map(_.getLong(0))
                .collect().sum / dateDiff(getFrontDay(date, 30), getFrontDay(date, 1))
            if (thisDayAll < frontDayAll * 0.8) {
                throw new Exception(s"当前印尼SABCD评分数据一共${thisDayAll}个，前30天平均${frontDayAll}个，数据异常")
            }
            else {
                println(s"当前印尼SABCD评分数据一共${thisDayAll}个，前30天平均${frontDayAll}个，数据正常")
            }
        }

        val v7S = getAppSetBc(sparkSession, locale, endDate, "S", "").value
        val v7A = getAppSetBc(sparkSession, locale, endDate, "A", "").value
        val v7B = getAppSetBc(sparkSession, locale, endDate, "B", "").value
        val v7C = getAppSetBc(sparkSession, locale, endDate, "", "C").value
        val v7D = getAppSetBc(sparkSession, locale, endDate, "", "D").value

        val recipients = Array("oversea-rec@xm.com")
        val subject =
            s"""
               | ${getCurrentDateStr()} ${locale2countryName(locale)}使用的app评分数据汇总
               |""".stripMargin
        val message =
            s"""
               | 最新一天的 dwd_ias_recommend_app_di 数据日期是${date}
               | --------------------------------------------------------
               | app评分 | 线上读取的${date}的数量
               | --------------------------------------------------------
               | ${v3}
               |
               | SAB使用过去7天的数据，CD使用最新的数据
               | --------------------------------------------------------
               | app评分      | 当前级别数量
               | --------------------------------------------------------
               | S            ${v7S.size}
               | A            ${v7A.size}
               | B            ${v7B.size}
               | C            ${v7C.size}
               | D            ${v7D.size}
               | SAB          ${v7S.union(v7A).union(v7B).size}
               | SABC         ${v7S.union(v7A).union(v7B).union(v7C).size}
               | SABCD        ${v7S.union(v7A).union(v7B).union(v7C).union(v7D).size}
               |""".stripMargin
        sendEMail(recipients, subject, message)
    }
}
