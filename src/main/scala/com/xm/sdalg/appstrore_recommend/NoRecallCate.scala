package com.xm.sdalg.appstrore_recommend

import com.xm.sdalg.appstrore_recommend.Utils._
import com.xm.sdalg.commons.ClusterUtils.{SG_OTHER, locale2catalog, locale2locale, localeInCluster, singapore}
import com.xm.sdalg.commons.HDFSUtils
import com.xm.sdalg.commons.HDFSUtils.rddSaveInHDFS
import com.xm.sdalg.commons.SparkContextUtils.createSparkSession
import com.xm.sdalg.commons.TimeUtils.dateSeqBetweenEndAndDiff
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

import scala.collection.immutable

/**
 * ABC 级别下未召回分类别数据
 */
object NoRecallCate {
    def main(args: Array[String]): Unit = {
        val Array(localeOrg: String, startDate: String, endDate: String) = args
        var locale = localeOrg
        if (localeInCluster(localeOrg, singapore) && localeOrg != "ID")
            locale = "ID"
        val sparkSession: SparkSession = createSparkSession(this.getClass.getSimpleName)
        import sparkSession.implicits._
        val recallAppSet: Broadcast[Set[String]] = sparkSession.sparkContext.broadcast(getRecall(sparkSession, locale, startDate, endDate))
        val allAppFilterSAB_CDV7: Broadcast[Set[String]] = getAppSetBc(sparkSession, locale, endDate, "A", "")
        val recallBlackSet = getRecallBlackSet(sparkSession, locale, endDate)
        val frontDf = getDownloadNumBySearch(sparkSession, locale, startDate, endDate, "score", allAppFilterSAB_CDV7, recallBlackSet)
            .selectExpr("package_name as pkg", "score")

        val package2init = package2intlCategoryId(sparkSession, locale).selectExpr("package_name as pkg ", "intl_category_id")

        var noRecallSetRdd = getLatestAppSetBC(sparkSession, locale)
            .filter(row => !recallAppSet.value.contains(row.getAs[String]("pkg")))
            .join(frontDf, Seq("pkg"))
            .join(package2init, Seq("pkg"))
            .where("level in ('A')")
            .selectExpr("intl_category_id", "level", "pkg", "score")
            .rdd
            .map(row => (Array("7DaysNoRecallCate", row.getString(1), row.getInt(0)).mkString("_"), (row.getString(2), row.getLong(3))))
            .groupByKey()
            .map(row => (row._1, row._2.toArray.sortBy(_._2).reverse.toMap))

        val noRecallSetDf = noRecallSetRdd.toDF("id", "apps")

        dateSeqBetweenEndAndDiff(endDate, 1).foreach(date => {
            val temp: RDD[(String, Map[String, Long])] = getFrontData(sparkSession, locale, date)
            if (temp != null) {
                noRecallSetRdd = noRecallSetRdd.union(temp)
            }
        })
        val front7DaysNoRecall = noRecallSetRdd
            .groupByKey()
            .map(row => {
                val temp = row._1.split("_")
                val id = Array(temp(0), "last7day", temp(1), temp(2)).mkString("_")
                val res: Map[String, Long] = row._2.flatten.toArray.map(i => (i._1, i._2)).groupBy(_._1).mapValues(_.map(_._2).sum)
                (id, res)
            })
            .toDF("id", "apps")

        val resultDf = noRecallSetDf
            .union(front7DaysNoRecall)

        resultDf.show(1000, false)
        resultDf.printSchema()
        rddSaveInHDFS(sparkSession, resultDf.toJSON.rdd, dim_appstore_recommend_tag_doc(endDate, locale2locale(localeOrg), "7DaysNoRecallCate"))

    }

    def getFrontData(sparkSession: SparkSession, locale: String, date: String) = {
        import sparkSession.implicits._
        if (HDFSUtils.exists(sparkSession.sparkContext, dim_appstore_recommend_tag_doc(date, locale2locale(locale), "7DaysNoRecallCate"))) {
            sparkSession.read.json(dim_appstore_recommend_tag_doc(date, locale2locale(locale), "7DaysNoRecallCate"))
                .selectExpr("id", "apps")
                .filter(row => !row.getString(0).contains("7DaysNoRecallCate_last7day_A_"))
                .rdd
                .map(row => {
                    val id = row.getString(0)
                    val temp = row.getAs[GenericRowWithSchema](1)
                    val apps: Map[String, Long] = Range(0, temp.schema.fieldNames.length)
                        .map(ii => {
                            if (temp.isNullAt(ii))
                                (temp.schema.fieldNames(ii), 0L)
                            else
                                (temp.schema.fieldNames(ii), temp.getLong(ii))
                        })
                        .filter(_._2 != 0L)
                        .distinct
                        .toMap
                    (id, apps)
                })
        } else null
    }

    def getRecall(sparkSession: SparkSession, locale: String, startDate: String, endDate: String) = {
        getRecallDf(sparkSession, locale, startDate, endDate)
            .filter(row => {
                val queue_all = row.getAs[String]("queue_all").split(",")
                queue_all.length > 1 || !queue_all(0).contains("7DaysNoRecallCate_last7day_")
            })
            .selectExpr("pkg").distinct()
            .rdd.map(_.getString(0))
            .collect().toSet

    }
}
