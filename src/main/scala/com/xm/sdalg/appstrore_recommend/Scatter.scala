package com.xm.sdalg.appstrore_recommend

import com.xm.sdalg.appstrore_recommend.Utils.{getAppSetBc, getAuditBlackSet, getLatestAppSetBC}
import com.xm.sdalg.commons.CalcUtils._
import com.xm.sdalg.commons.ClusterUtils.{filterLocale, locale2catalog, locale2locale}
import com.xm.sdalg.commons.HDFSUtils.{getLastIcebergDate, getLatestExistDate, getLatestExistPath, readLastDateIceberg}
import com.xm.sdalg.commons.SimilarityUtils.getLevenshteinDistance
import com.xm.sdalg.commons.SparkContextUtils.createSparkSession
import com.xm.sdalg.commons.TimeUtils.{getCurrentDateStr, getFrontDay}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 打散数据
 */
object Scatter {
    def main(args: Array[String]): Unit = {
        val Array(locale: String, endDate: String) = args
        val sparkSession: SparkSession = createSparkSession(this.getClass.getSimpleName)
        import sparkSession.implicits._
        val allAppSetBc = getAppSetBc(sparkSession, locale, endDate, "SAB", "CD")
        val allSA: Broadcast[Set[String]] = getAppSetBc(sparkSession, locale, endDate, "AS", "")
        val allBCD: Broadcast[Set[String]] = getAppSetBc(sparkSession, locale, endDate, "", "BCD")
        // title 打散
        val simAppByTitle = sparkSession.read.text(getLatestExistPath(sparkSession, s"/user/h_data_platform/platform/appstore/dim_appstore_faiss/tag=mulitmod_title_embedding/date=yyyyMMdd", getCurrentDateStr(), 365))
            .rdd
            .map(_.getString(0).split(","))
            .filter(_.length == 3)
            .map(row => (row(0), (row(2), row(1).toDouble)))
            .groupByKey()
            .map(row => (row._1, row._2.toArray.sortBy(_._2).slice(0, 20).map(_._1).mkString(","))) // 欧式距离越小越相近
            .toDF("packageName", "simAppByTitle")

        // icon 相似结果做打散
        val iconDate = getLatestExistDate(sparkSession, s"/user/h_data_platform/platform/appstore/dim_appstore_faiss/tag=icon_emb_${locale}/date=yyyyMMdd", getCurrentDateStr(), 365)
        val simAppByIconClip = sparkSession.read.text(s"/user/h_data_platform/platform/appstore/dim_appstore_faiss/tag=icon_emb_ID/date=${iconDate}/res.csv")
            .rdd
            .map(_.getString(0).split(","))
            .filter(_.length == 3)
            .map(row => (row(0), (row(2), row(1).toDouble)))
            .groupByKey()
            .map(row => (row._1, row._2.toArray.sortBy(_._2).slice(0, 20).map(_._1).mkString(",")))
            .toDF("packageName", "simAppByIconClip") // 图片相似
        val simAppByIconClipV2 = sparkSession.read.text(s"/user/h_data_platform/platform/appstore/dim_appstore_faiss/tag=icon_emb_ID/date=${iconDate}/res.csv")
            .rdd
            .map(_.getString(0).split(","))
            .filter(_.length == 3)
            .map(row => (row(0), (row(2), row(1).toDouble)))
            .groupByKey()
            .flatMap(row => {
                val packageName = row._1
                val simApp = row._2.toArray.sortBy(_._2).slice(0, 10).map(_._1)
                simApp.flatMap(i => Array((packageName, i), (i, packageName)))
            })
            .filter(row => row._1 != row._2)
            .groupByKey()
            .map(row => (row._1, row._2.toArray.mkString(",")))
            .toDF("packageName", "simAppByIconClipV2") // 图片相似

        var result = simAppByTitle.join(simAppByIconClip, Seq("packageName"), "outer")
            .selectExpr("packageName", "simAppByTitle", "simAppByIconClip")
            .rdd
            .flatMap(row => {
                val packageName = row.getString(0)
                val simAppByTitle = {
                    if (!row.isNullAt(1))
                        row.getString(1).split(",")
                    else
                        Array[String]()
                }
                val simAppByIconClip = {
                    if (!row.isNullAt(2))
                        row.getString(2).split(",")
                    else
                        Array[String]()
                }
                val all = (simAppByTitle ++ simAppByIconClip).toSet
                all.flatMap(i => {
                    Array((packageName, i), (i, packageName))
                })
            })
            .map(row => {
                val packageName = row._1
                val simApp = row._2
                (packageName, simApp)
            })
            .groupByKey()
            .map(row => {
                val packageName = row._1
                val simApp = row._2.toArray.distinct.mkString(",")
                (packageName, simApp)
            })
            .toDF("packageName", "scatterApps")
        val developerId = getSensitiveDev(sparkSession, locale)
        val sameDev = getSameDev(sparkSession, locale)
        val simTitle = getSimAppByLevenshtein(sparkSession, locale)
        val simTitleV2 = getSimAppByLevenshteinV2(sparkSession, locale)
        val simDeveloper = getSimDeveloper(sparkSession, locale)

        result = result
            .join(developerId, Seq("packageName"), "outer")
            .join(sameDev, Seq("packageName"), "outer")
            .join(simTitle, Seq("packageName"), "outer")
            .join(simTitleV2, Seq("packageName"), "outer")
            .join(simAppByTitle, Seq("packageName"), "outer")
            .join(simAppByIconClip, Seq("packageName"), "outer")
            .join(simDeveloper, Seq("packageName"), "outer")
            .join(simAppByIconClipV2, Seq("packageName"), "outer")

            .withColumn("scatterAppsV5", scatterAppsV2UDF(col("simAppByLevenshteinV2"), col("simAppByIconClip"), lit(null)))
            .withColumn("scatterAppsV7", scatterAppsV2UDF(col("simAppByLevenshteinV2"), col("simAppByIconClipV2"), lit(null)))
            .withColumn("scatterAppsV6", scatterAppsV2UDF(col("simAppByLevenshteinV2"), col("simAppByIconClip"), col("simPublishername")))
            .withColumn("scatterAppsV8", scatterAppsV2UDF(col("simAppByLevenshteinV2"), col("simAppByIconClipV2"), col("simPublishername")))
            .withColumn("scatterAppsV9", scatterSS(allSA, allBCD)(col("packageName"), col("scatterAppsV5")))
            .selectExpr("packageName", "scatterAppsV5", "scatterAppsV6", "scatterAppsV7", "scatterAppsV8", "scatterAppsV9")
            .filter(row => allAppSetBc.value.contains(row.getString(0)))

        result.show(false)
        result.printSchema()
        metric(sparkSession, result.selectExpr("packageName", "scatterAppsV5"), "scatterAppsV5")
        metric(sparkSession, result.selectExpr("packageName", "scatterAppsV6"), "scatterAppsV6")
        metric(sparkSession, result.selectExpr("packageName", "scatterAppsV7"), "scatterAppsV7")
        metric(sparkSession, result.selectExpr("packageName", "scatterAppsV8"), "scatterAppsV8")
        metric(sparkSession, result.selectExpr("packageName", "scatterAppsV9"), "scatterAppsV9")
        result.write.mode("overwrite").parquet(s"/user/h_data_platform/platform/appstore/dwd_appstore_recommend_rerank/${locale2locale(locale)}")
    }

    def scatterSS(allS: Broadcast[Set[String]], allBCD: Broadcast[Set[String]]): UserDefinedFunction = {
        udf((a: String, b: String) => {
            b.split(",").filter(allBCD.value.contains).mkString(",")
        })
    }

    def scatterAppsV2UDF: UserDefinedFunction = {
        udf((a: String, b: String, c: String) => {
            val aSet =
                if (a != null)
                    a.split(",").toSet
                else
                    Set[String]()
            val bSet =
                if (b != null)
                    b.split(",").toSet
                else
                    Set[String]()
            val cSet =
                if (c != null)
                    c.split(",").toSet
                else
                    Set[String]()
            val all = (aSet ++ bSet ++ cSet).toSet.filter(_.nonEmpty)
            all.mkString(",")
        })
    }

    def getSensitiveDev(sparkSession: SparkSession, locale: String): DataFrame = {
        val sensitiveApp: Broadcast[Set[String]] = getAuditBlackSet(sparkSession, locale)
        val sensitiveDev: Set[Long] = sparkSession.sql(
            s"""
               |SELECT
               |    packagename as packageName,developerId
               |FROM
               |    ${locale2catalog(locale, "hive")}.appstore.appstore_appinfo
               |""".stripMargin)
            .filter(row => sensitiveApp.value.contains(row.getString(0)))
            .selectExpr("developerId")
            .rdd
            .map(row => row.getLong(0))
            .collect()
            .toSet -- Set(0L, -1L)
        println("当前线上需要被过滤的开发者id: " + sensitiveDev.mkString(","))

        val date = getLastIcebergDate(sparkSession, s"${locale2catalog(locale, "iceberg")}.appstore.dwd_ias_recommend_app_di")
        val developerId = sparkSession.table(s"${locale2catalog(locale, "iceberg")}.appstore.dwd_ias_recommend_app_di")
            .where(s"date = '${date}'")
            .where(filterLocale("locale", locale))
            .selectExpr("packagename", "developerId")
            .withColumn("isSensitiveDev",
                when(col("developerId").isin(sensitiveDev.toArray: _*), true).otherwise(false))
            .selectExpr("packagename", "isSensitiveDev")
            .where("isSensitiveDev = true")
        println("当前线上需要被过滤的应用包名个数: " + developerId.where("isSensitiveDev = true").count())
        developerId
    }

    def getSameDev(sparkSession: SparkSession, locale: String): DataFrame = {
        import sparkSession.implicits._
        val date = getLastIcebergDate(sparkSession, s"${locale2catalog(locale, "iceberg")}.appstore.dwd_ias_recommend_app_di")
        val org = sparkSession.table(s"${locale2catalog(locale, "iceberg")}.appstore.dwd_ias_recommend_app_di")
            .where(s"date = '${date}'")
            .where(filterLocale("locale", locale))
            .selectExpr("packagename", "developerId")
            .where("developerId != 0")
            .repartition(1000)

        val res = org.selectExpr("packagename as packagename1", "developerId as developerId1")
            .crossJoin(org.selectExpr("packagename as packagename2", "developerId as developerId2"))
            .selectExpr("packagename1", "packagename2", "developerId1", "developerId2")
            .where("developerId1 = developerId2")
            .where("packagename1 != packagename2")
            .rdd
            .map(row => {
                val packageName1 = row.getString(0)
                val packageName2 = row.getString(1)
                (packageName1, packageName2)
            })
            .groupByKey()
            .map(row => {
                val id = row._1
                val simApp = row._2.toArray.distinct.mkString(",")
                (id, simApp)
            })
            .toDF("packageName", "simAppByDev")
        res
    }

    def getSimAppByLevenshtein(sparkSession: SparkSession, locale: String, minDistance: Double = 0.2): DataFrame = {
        import sparkSession.implicits._
        val date = getLastIcebergDate(sparkSession, s"${locale2catalog(locale, "iceberg")}.appstore.dwd_ias_recommend_app_di")
        val org = sparkSession.table(s"${locale2catalog(locale, "iceberg")}.appstore.dwd_ias_recommend_app_di")
            .where(s"date = '${date}'")
            .where(filterLocale("locale", locale))
            .selectExpr("packagename", "displayname")
            .rdd
            .map(row => {
                val packageName = row.getString(0)
                val displayName = row.getString(1).replaceAll(" ", "").toLowerCase()
                (packageName, displayName)
            })
            .filter(row => 4 < row._2.length && row._2.length < 20)
            .toDF("packageName", "displayName")
            .repartition(1000)

        val res = org.selectExpr("packageName as packageName1", "displayName as displayName1")
            .crossJoin(org.selectExpr("packageName as packageName2", "displayName as displayName2"))
            .selectExpr("packageName1", "packageName2", "displayName1", "displayName2")
            .where("packageName1 != packageName2")
            .rdd
            .map(row => {
                val packageName1 = row.getString(0)
                val packageName2 = row.getString(1)
                val displayName1 = row.getString(2)
                val displayName2 = row.getString(3)
                val levenshtein = getLevenshteinDistance(displayName1, displayName2)
                val rate = levenshtein / displayName1.length
                (packageName1, packageName2, displayName1, displayName2, displayName1.length, rate)
            })
            .filter(_._6 <= minDistance)
            .map(row => {
                val packageName1 = row._1
                val packageName2 = row._2
                (packageName1, packageName2)
            })
            .groupByKey()
            .map(row => {
                val id = row._1
                val simApp = row._2.toArray.distinct.mkString(",")
                (id, simApp)
            })
            .toDF("packageName", "simAppByLevenshtein")
        res
    }

    def getSimAppByLevenshteinV2(sparkSession: SparkSession, locale: String, minDistance: Double = 1): DataFrame = {
        import sparkSession.implicits._
        val org = readLastDateIceberg(sparkSession, s"${locale2catalog(locale, "iceberg")}.appstore.dwd_ias_recommend_app_di")
            .where(filterLocale("locale", locale))
            .selectExpr("packagename", "displayname", "level")
            .filter(row => 4 <= row.getString(1).length)
            .repartition(1000)

        val res = org.selectExpr("packageName as packageName1", "displayName as displayName1", "level as level1")
            .crossJoin(org.selectExpr("packageName as packageName2", "displayName as displayName2", "level as level2"))
            .selectExpr("packageName1", "packageName2", "displayName1", "displayName2", "level1", "level2")
            .where("packageName1 != packageName2")
            .rdd
            .map(row => {
                val packageName1 = row.getString(0)
                val packageName2 = row.getString(1)
                val displayName1 = row.getString(2).toLowerCase
                val displayName2 = row.getString(3).toLowerCase
                val levenshtein = getLevenshteinDistance(displayName1, displayName2)
                (packageName1, packageName2, displayName1, displayName2, levenshtein)
            })
            .filter(row => row._5 <= minDistance)
            .map(row => {
                val packageName1 = row._1
                val packageName2 = row._2
                (packageName1, packageName2)
            })
            .groupByKey()
            .map(row => {
                val id = row._1
                val simApp = row._2.toArray.distinct.mkString(",")
                (id, simApp)
            })
            .toDF("packageName", "simAppByLevenshteinV2")
        res
    }

    def getSimAppByJaccard(sparkSession: SparkSession, locale: String) = {
        import sparkSession.implicits._
        val date = getLastIcebergDate(sparkSession, s"${locale2catalog(locale, "iceberg")}.appstore.dwd_ias_recommend_app_di")
        val org = sparkSession.table(s"${locale2catalog(locale, "iceberg")}.appstore.dwd_ias_recommend_app_di")
            .where(s"date = '${date}'")
            .where(filterLocale("locale", locale))
            .filter(row => 4 < row.getString(1).length)
            .selectExpr("packagename", "displayname")

        val sa = org.where("level in ('S','A') ")
            .selectExpr("packagename as packagename1", "displayname as displayname1")
        val bcd = org.where("level in ('B','C','D') ")
            .selectExpr("packagename as packagename2", "displayname as displayname2")


        val pacSim = sa.crossJoin(bcd)
            .selectExpr("packagename1", "packagename2", "displayname1", "displayname2")
    }


    def getSimDeveloper(sparkSession: SparkSession, locale: String) = {
        import sparkSession.implicits._
        val org = readLastDateIceberg(sparkSession, s"${locale2catalog(locale, "iceberg")}.appstore.dwd_ias_recommend_app_di")
            .where(filterLocale("locale", locale))
            .selectExpr("packagename", "publishername", "intlcategoryid", "displayName")

        val res = org.selectExpr("packagename as packagename1", "publishername as publishername1", "intlcategoryid as intlcategoryid1", "displayName as displayName1")
            .crossJoin(org.selectExpr("packagename as packagename2", "publishername as publishername2", "intlcategoryid as intlcategoryid2", "displayName as displayName2"))
            .where("packagename1 != packagename2")
            .where("intlcategoryid1 = intlcategoryid2")
            .where("publishername1 = publishername2")
            .selectExpr("packageName1", "packageName2", "displayName1", "displayName2")
            .rdd
            .map(row => {
                val packageName1 = row.getString(0)
                val packageName2 = row.getString(1)
                val displayName1 = row.getString(2).toLowerCase()
                val displayName2 = row.getString(3).toLowerCase()
                val levenshtein = getLevenshteinDistance(displayName1, displayName2)
                val rate = levenshtein / displayName1.length
                (packageName1, packageName2, displayName1, displayName2, levenshtein, rate)
            })
            .filter(row => 4 < row._3.length)
            .filter(_._6 <= 0.2)
            .filter(_._5 <= 2)
            .toDF("packageName1", "packageName2", "displayName1", "displayName2", "levenshtein", "rate")
            .selectExpr("packagename1", "packagename2")
            .rdd
            .map(row => {
                val packageName1 = row.getString(0)
                val packageName2 = row.getString(1)
                (packageName1, packageName2)
            })
            .groupByKey()
            .map(row => {
                val id = row._1
                val simApp = row._2.toArray.distinct.mkString(",")
                (id, simApp)
            })
            .toDF("packageName", "simPublishername")
        metric(sparkSession, res, "publishername")
        res
    }

    def metric(sparkSession: SparkSession, res: DataFrame, name: String) = {
        import sparkSession.implicits._
        val sort = res
            .rdd
            .map(row => (row.getString(0), row.getString(1).split(",").length))

        println(
            s"""
               |当前的打散方式是 ${name}
               |共有 ${res.count()} 有需要被打散的应用
               |其中数量最多的前5的应用是 ${sort.sortBy(-_._2).take(5).mkString(",")}
               |其中数量最少的前5的应用是 ${sort.sortBy(_._2).take(5).mkString(",")}
               |一共有 ${sort.map(_._2).sum()} 个应用要被打散
               |""".stripMargin)
    }
}
