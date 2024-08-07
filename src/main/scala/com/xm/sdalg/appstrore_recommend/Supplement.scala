package com.xm.sdalg.appstrore_recommend

import com.google.gson.JsonParser
import com.xm.sdalg.appstrore_recommend.Utils._
import com.xm.sdalg.commons.ClusterUtils._
import com.xm.sdalg.commons.HDFSUtils._
import com.xm.sdalg.commons.SparkContextUtils._
import com.xm.sdalg.commons.TimeUtils._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, desc, explode, from_json}
import org.apache.spark.sql.types.{ArrayType, StringType}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
 * 兜底数据
 */
object Supplement {
    def main(args: Array[String]): Unit = {
        val Array(locale: String, date: String) = args
        val sparkSession: SparkSession = createSparkSession(this.getClass.getSimpleName)
        import sparkSession.implicits._

        val scoreDf = readParquet4dwd_app_stat_event_di(sparkSession, getFrontDay(30), getFrontDay(1), locale)
            .where(s"event_type in ('first_launch')")
            .groupBy("package_name").count()
            .selectExpr("package_name as packagename", "count")
        val org = readLastDateIceberg(sparkSession, s"${locale2catalog(locale, "iceberg")}.appstore.dwd_ias_recommend_app_di")
            .where("level in ('S','A','B','C')")
            .join(scoreDf, Seq("packagename"), "left")
            .selectExpr("packagename", "locale", "intlcategoryid", "count", "developerId", "tagids")
            .na.fill(Map("count" -> new Random().nextDouble().toLong))
            .cache()

        var result: DataFrame = null
        val supplement_locale = org
            .rdd
            .map(row => {
                val pkg = row.getString(0)
                val locale = row.getString(1)
                val score = row.getLong(3).toDouble
                (s"supplement_${locale}", (pkg, score))
            })
            .groupByKey()
            .map(row => {
                val id = row._1
                val apps = row._2.toArray.sortBy(_._2).reverse.slice(0, 1000).toMap
                (id, apps)
            })
            .toDF("id", "apps")
        val supplement_locale_intlCategoryId = org
            .rdd
            .map(row => {
                val pkg = row.getString(0)
                val locale = row.getString(1)
                val intlCategoryId = row.getInt(2)
                val score = row.getLong(3).toDouble
                (s"supplement_${locale}_${intlCategoryId}", (pkg, score))
            })
            .groupByKey()
            .map(row => {
                val id = row._1
                val apps = row._2.toArray.sortBy(_._2).reverse.slice(0, 1000).toMap
                (id, apps)
            })
            .toDF("id", "apps")
        val supplement_locale_tag = org
            .withColumn("tagid", explode(from_json(col("tagids"), ArrayType(StringType))))
            .where("tagid is not null")
            .selectExpr("packagename", "locale", "intlcategoryid", "count", "developerId", "tagid")
            .rdd
            .map(row => {
                val pkg = row.getString(0)
                val locale = row.getString(1)
                val tag = row.getString(5)
                val score = row.getLong(3).toDouble
                (s"supplement_${locale}_${tag}", (pkg, score))
            })
            .groupByKey()
            .map(row => {
                val id = row._1
                val apps = row._2.toArray.sortBy(_._2).reverse.slice(0, 1000).toMap
                (id, apps)
            })
            .toDF("id", "apps")
        val supplement_locale_game = org
            .where(s"intlcategoryid > 100")
            .rdd
            .map(row => {
                val pkg = row.getString(0)
                val locale = row.getString(1)
                val score = row.getLong(3).toDouble
                (s"supplement_${locale}_game", (pkg, score))
            })
            .groupByKey()
            .map(row => {
                val id = row._1
                val apps = row._2.toArray.sortBy(_._2).reverse.slice(0, 1000).toMap
                (id, apps)
            })
            .toDF("id", "apps")
        val supplement_locale_app = org
            .where(s"0 < intlcategoryid and intlcategoryid < 100")
            .rdd
            .map(row => {
                val pkg = row.getString(0)
                val locale = row.getString(1)
                val score = row.getLong(3).toDouble
                (s"supplement_${locale}_app", (pkg, score))
            })
            .groupByKey()
            .map(row => {
                val id = row._1
                val apps = row._2.toArray.sortBy(_._2).reverse.slice(0, 1000).toMap
                (id, apps)
            })
            .toDF("id", "apps")


        val supplement_locale_dev = org
            .selectExpr("packagename", "locale", "developerId", "count")
            .where("developerId > 0")
            .rdd
            .map(row => {
                val pkg = row.getString(0)
                val locale = row.getString(1)
                val score = row.getLong(3).toDouble
                (s"supplement_${locale}_dev", (pkg, score))
            })
            .groupByKey()
            .map(row => (row._1, row._2.toList.sortBy(_._2).reverse.take(1000).toMap))
            .toDF("id", "apps")

        result = union(result, supplement_locale_intlCategoryId)
        result = union(result, supplement_locale_tag)
        result = union(result, supplement_locale_game)
        result = union(result, supplement_locale_app)
        result = union(result, supplement_locale_dev)
        result = union(result, supplement_locale)
        result
            .selectExpr("id", "apps")
            .rdd
            .map(row => {
                val id = row.getString(0)
                val apps = row.getMap[String, Double](1)
                (id, apps.keySet.size)
            })
            .collect()
            .foreach(row => println(row._1, row._2))
        rddSaveInHDFS(sparkSession, result.toJSON.rdd, dim_appstore_recommend_tag_doc(date, locale2locale(locale), "supplement"), numPartitions = 1)
        rddSaveInHDFS(sparkSession, result.toJSON.rdd, dim_appstore_recommend_tag_doc("supplement", "supplement", "supplement"), numPartitions = 1)
    }
}
