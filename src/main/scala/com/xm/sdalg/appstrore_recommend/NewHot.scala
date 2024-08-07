package com.xm.sdalg.appstrore_recommend

import com.xm.sdalg.appstrore_recommend.HotItemInCate.getHotItemInCate
import com.xm.sdalg.appstrore_recommend.Utils._
import com.xm.sdalg.commons.ClusterUtils._
import com.xm.sdalg.commons.CubeUtils._
import com.xm.sdalg.commons.HDFSUtils
import com.xm.sdalg.commons.HDFSUtils._
import com.xm.sdalg.commons.SparkContextUtils._
import com.xm.sdalg.commons.TimeUtils._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object NewHot {
    val topN = 200
    val orgCate = "platNewHot"

    def main(args: Array[String]): Unit = {
        val Array(locale: String, endDate: String, rate: String) = args
        val sparkSession: SparkSession = createSparkSession(this.getClass.getSimpleName)
        var resultDf: Dataset[Row] = null
        if (locale == "ID") {
            val allAppFilterSAB_CDV9: Broadcast[Set[String]] = getAppSetBc(sparkSession, locale, endDate, "SA", "")
            resultDf = union(resultDf, getNewHot(sparkSession, locale, endDate, rate, allAppFilterSAB_CDV9, "platNewHot_V9_"))
        } else {
            val allAppFilterSAB_CDV7 = getAppSetBc(sparkSession, locale, endDate, "SA", "")
            resultDf = union(resultDf, getNewHot(sparkSession, locale, endDate, rate, allAppFilterSAB_CDV7, "platNewHot"))
        }
        resultDf.show(false)
        rddSaveInHDFS(sparkSession, resultDf.toJSON.rdd, dim_appstore_recommend_tag_doc(endDate, locale2locale(locale), orgCate))
    }

    def getNewHot(sparkSession: SparkSession, locale: String, endDate: String, rate: String,
                  allAppFilterSAB_CDV7: Broadcast[Set[String]],
                  name: String) = {
        import sparkSession.implicits._
        val frontDf = getDownloadNumByPlat(sparkSession, locale, getFrontDay(endDate, 7), getFrontDay(endDate, 1), "frontDay", allAppFilterSAB_CDV7)
        val thisDf = getDownloadNumByPlat(sparkSession, locale, endDate, endDate, "thisDay", allAppFilterSAB_CDV7)
        val temp = frontDf
            .join(thisDf, Seq("package_name"), "outer")
            .where(s"thisDay >= 500")
            .withColumn("bili", chufaUDF(col("thisDay"), lit(dateDiff(getFrontDay(endDate, 7), getFrontDay(endDate, 1))), col("frontDay")))
            .where(s"bili >= 1 + ${rate.toDouble}")
            .where(s"frontDay > 500")
            .selectExpr("package_name", "bili")
        val orgResult = temp
            .rdd
            .map(row => (name, (row.getString(0), row.getDouble(1))))
            .groupByKey()
            .map(row => (row._1, row._2.toArray.slice(0, topN).toMap))
            .toDF("id", "apps")

        val mergeDf = merge(sparkSession, locale, endDate, name, orgCate)
        val mergeResult =
            if (mergeDf == null)
                temp
                    .rdd
                    .map(row => (s"${name}V2", (row.getString(0), row.getDouble(1))))
                    .groupByKey()
                    .map(row => (row._1, row._2.toArray.slice(0, topN).toMap))
                    .toDF("id", "apps")
            else
                temp
                    .unionByName(mergeDf)
                    .rdd
                    .map(row => (row.getString(0), row.getDouble(1)))
                    .reduceByKey(Math.max)
                    .map(row => (s"${name}V2", (row._1, row._2)))
                    .groupByKey()
                    .map(row => (row._1, row._2.toArray.slice(0, topN).toMap))
                    .toDF("id", "apps")
        orgResult.union(mergeResult)
    }


    def getDownloadNumBySearch(sparkSession: SparkSession, locale: String, startDate: String, endDate: String, name: String, whiteSet: Broadcast[Set[String]]) = {
        sparkSession.read.parquet(dwd_appstore_search_di)
            .where(s"date between $startDate and $endDate")
            .where(filterLocale("lo", locale))
            .where(s"package_name is not null")
            .where(s"action_type in ('download_install','download')")
            .filter(row => whiteSet.value.contains(row.getAs[String]("package_name")))
            .groupBy("package_name").agg(countDistinct("gaid").as(name))
    }

    def getDownloadNumByPlat(sparkSession: SparkSession, locale: String, startDate: String, endDate: String, name: String, whiteSet: Broadcast[Set[String]]) = {
        readParquet4dwd_app_stat_event_di(sparkSession, startDate, endDate, locale)
            .where(s"event_type in ('first_launch')")
            .where(filterLocale("region", locale))
            .filter(row => whiteSet.value.contains(row.getAs[String]("package_name")))
            .selectExpr("imeimd5", "package_name")
            .groupBy("package_name").agg(countDistinct("imeimd5").as(name))

    }

    val chufaUDF: UserDefinedFunction = {
        udf((a: Long, num: Int, b: Long) => {
            if (b != 0)
                a.toDouble / (b.toDouble / num)
            else
                a
        })
    }

    def merge(sparkSession: SparkSession, locale: String, endDate: String, name: String, orgCate: String, dateDiff: Int = 7) = {
        import sparkSession.implicits._
        var allDataframe: DataFrame = null
        dateSeqBetweenStartAndEnd(getFrontDay(endDate, dateDiff), endDate).foreach(date => {
            if (HDFSUtils.exists(sparkSession.sparkContext, dim_appstore_recommend_tag_doc(date, locale2locale(locale), orgCate))) {
                val org: RDD[(String, Map[String, Double])] = getMapFromDocPath[Double](sparkSession, locale, dim_appstore_recommend_tag_doc(date, locale2locale(locale), orgCate), dataType = "Double", key = "id", value = "apps", blackKeySet = null, whiteKeySet = null)
                val temp = org
                    .map(row => (row._1, row._2))
                    .filter(_._1.equals(name))
                    .flatMapValues(_.toArray.sortBy(_._2).reverse.slice(0, topN))
                    .map(row => (row._2._1, row._2._2))
                    .toDF("package_name", "bili")
                if (allDataframe == null)
                    allDataframe = temp
                else
                    allDataframe = allDataframe.union(temp)
            }
        })
        allDataframe
    }
}
