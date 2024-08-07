package com.xm.sdalg.appstrore_recommend

import com.xm.sdalg.appstrore_recommend.Utils._
import com.xm.sdalg.commons.ClusterUtils.{SG_OTHER, locale2locale, localeInCluster, singapore}
import com.xm.sdalg.commons.HDFSUtils
import com.xm.sdalg.commons.HDFSUtils.rddSaveInHDFS
import com.xm.sdalg.commons.SparkContextUtils.createSparkSession
import com.xm.sdalg.commons.TimeUtils.dateSeqBetweenEndAndDiff
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

/**
 * 海外商店推荐 高等级未召回应用
 */
object NoRecallSet {
    def main(args: Array[String]): Unit = {
        val Array(localeOrg: String, startDate: String, endDate: String) = args
        var locale = localeOrg
        if (localeInCluster(localeOrg, singapore) && localeOrg != "ID")
            locale = "ID"
        val sparkSession: SparkSession = createSparkSession(this.getClass.getSimpleName)
        import sparkSession.implicits._
        val recallAppSet: Broadcast[Set[String]] = sparkSession.sparkContext.broadcast(getRecall(sparkSession, locale, startDate, endDate))
        val allAppFilterSAB_CDV7: Broadcast[Set[String]] = getAppSetBc(sparkSession, locale, endDate, "S", "")
        val recallBlackSet = getRecallBlackSet(sparkSession, locale, endDate)
        val frontDf = getDownloadNumBySearch(sparkSession, locale, startDate, endDate, "score", allAppFilterSAB_CDV7, blackSet = recallBlackSet)
            .selectExpr("package_name as pkg", "score")
        var noRecallSetRdd = getLatestAppSetBC(sparkSession, locale)
            .filter(row => !recallAppSet.value.contains(row.getAs[String]("pkg")))
            .join(frontDf, Seq("pkg"))
            .selectExpr("level", "pkg", "score")
            .where("level in ('S')")
            .rdd
            .map(row => ("7DaysNoRecallSet_" + row.getString(0), (row.getString(1), row.getLong(2))))
            .groupByKey()
            .map(row => (row._1, row._2.map(i => (i._1, i._2)).toMap))
        val noRecallSetDf = noRecallSetRdd.toDF("id", "apps")

        dateSeqBetweenEndAndDiff(endDate, 7).foreach(date => {
            val temp: RDD[(String, Map[String, Long])] = getFrontData(sparkSession, locale, date)
            if (temp != null) {
                noRecallSetRdd = noRecallSetRdd.union(temp)
            }
        })
        val front7DaysNoRecall = noRecallSetRdd
            .groupByKey()
            .map(row => {
                val id = Array(row._1.split("_")(0), "last7day", row._1.split("_")(1)).mkString("_")
                val res: Map[String, Long] = row._2.flatten.toArray.map(i => (i._1, i._2)).groupBy(_._1).mapValues(_.map(_._2).sum)
                (id, res)
            })
            .toDF("id", "apps")

        val resultDf = noRecallSetDf
            .union(front7DaysNoRecall)

        resultDf.show(false)
        resultDf.printSchema()
        rddSaveInHDFS(sparkSession, resultDf.toJSON.rdd, dim_appstore_recommend_tag_doc(endDate, locale2locale(localeOrg), "7DaysNoRecallSet"))

    }

    def getFrontData(sparkSession: SparkSession, locale: String, date: String) = {
        if (HDFSUtils.exists(sparkSession.sparkContext, dim_appstore_recommend_tag_doc(date, locale2locale(locale), "7DaysNoRecallSet"))) {
            sparkSession.read.json(dim_appstore_recommend_tag_doc(date, locale2locale(locale), "7DaysNoRecallSet"))
                .selectExpr("id", "apps")
                .where("id in ('7DaysNoRecallSet_S')")
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
                        .toMap
                    (id, apps)
                })
        } else null
    }

    def getRecall(sparkSession: SparkSession, locale: String, startDate: String, endDate: String) = {
        getRecallDf(sparkSession, locale, startDate, endDate)
            .filter(row => {
                val queue_all = row.getAs[String]("queue_all").split(",")
                queue_all.length > 1 || queue_all(0) != "7DaysNoRecallSet_last7day_S" // 除了S级全部召回以外的其他队列

            })
            .selectExpr("pkg").distinct()
            .rdd.map(_.getString(0))
            .collect().toSet

    }
}
