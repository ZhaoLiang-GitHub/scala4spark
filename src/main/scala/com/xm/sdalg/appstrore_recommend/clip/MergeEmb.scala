package com.xm.sdalg.appstrore_recommend.clip

import com.xm.sdalg.commons.SparkContextUtils.createSparkSession
import com.xm.sdalg.commons.TimeUtils._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 将新的icon_embedding和旧的icon_embedding合并
 * 如果新的icon_embedding中有旧的icon_embedding，则使用新的icon_embedding
 * 没有的话，使用旧的icon_embedding
 */
object MergeEmb {
    def main(args: Array[String]): Unit = {
        val Array(locale: String, date: String) = args.slice(0, 2)
        val sparkSession: SparkSession = createSparkSession(this.getClass.getSimpleName)
        import sparkSession.implicits._
        val newEmb = sparkSession.read.text("/user/h_sns/zhaoliang6/clip/emb/all")
            .rdd
            .map(_.getString(0).trim.split("\t"))
            .filter(_.length == 2)
            .filter(_ (1).split(",").length == 512)
            .map(row => (row(0), row(1).split(",").map(_.toDouble)))
            .toDF("icon_id", "icon_embedding")
            .join(icon2package(sparkSession), Seq("icon_id"))
            .selectExpr("pkg", "icon_embedding")
        newEmb.show(false)

        val oldEmb = sparkSession.read.parquet("/user/h_data_platform/platform/appstore/dwd_appstore_recommend_icon_emb")
            .selectExpr("pkg", "icon_embedding")
            .rdd
            .map(row => (row.getString(0), row.getSeq[Double](1)))
            .toDF("pkg", "icon_embedding")
        oldEmb.show(false)
        val both = oldEmb.join(newEmb, Seq("pkg"), "inner")
        val res = oldEmb
            .join(both, Seq("pkg"), "left_anti")
            .union(newEmb)
            .selectExpr("pkg", "icon_embedding")
        res.show(false)
        res.printSchema()
        println(
            s"""
               |原有向量数量 ${oldEmb.count()}
               |新增向量数量 ${newEmb.join(both, Seq("pkg"), "left_anti").count()}
               |更新向量数量 ${both.count()}
               |最终向量数量 ${res.count()}
               |""".stripMargin)
        //        res.write.mode("overwrite").parquet("/user/h_data_platform/platform/appstore/dwd_appstore_recommend_icon_emb")
    }


    def icon2package(sparkSession: SparkSession) = {
        sparkSession.sql(
            s"""
               |SELECT
               |    packagename as pkg,
               |    REPLACE(icon, 'AppStore/', '') as icon_id
               |FROM
               |    hive_alsgprc_hadoop.appstore.appstore_appinfo
               |group by
               |    packagename,
               |    icon
               |""".stripMargin)

    }
}