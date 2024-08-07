package com.xm.sdalg.appstrore_recommend

import com.xm.sdalg.appstrore_recommend.Utils._
import com.xm.sdalg.commons.CalcUtils._
import com.xm.sdalg.commons.ClusterUtils._
import com.xm.sdalg.commons.HDFSUtils._
import com.xm.sdalg.commons.SparkContextUtils.createSparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType}

import scala.collection.mutable.Seq

/**
 * @zhaoliang6 on 20231024
 *             用户喜欢的tag分类顺序
 */
object UserHotTag {
    def main(args: Array[String]): Unit = {
        val Array(locale: String, startDate: String, endDate: String) = args
        val sparkSession: SparkSession = createSparkSession(this.getClass.getSimpleName)
        import sparkSession.implicits._
        val package2idDf = getPackage2Tags(sparkSession, locale)
            .selectExpr("pkg as package_name", "tagid as intl_category_id")
        val resultDf = readParquet4dwd_appstore_client_track(sparkSession, startDate, endDate, locale)
            .where("gaid is not null and gaid != '' and package_name is not null and package_name != ''")
            .where("action_type in ('item_exposure','item_click')")
            .selectExpr("gaid", "package_name as packageName", "action_type as behaviorType", "date")
            .distinct()
            .groupBy("gaid", "packageName", "behaviorType").count()
            .join(package2idDf.selectExpr("package_name as packageName", "intl_category_id as intlCategoryId"), Seq("packageName"))
            .where(s"intlCategoryId is not null")
            .groupBy("gaid", "intlCategoryId").agg(collect_list(struct("behaviorType", "count")).as("struct"))
            .rdd
            .map(r => {
                val struct = r.getAs[collection.mutable.WrappedArray[GenericRowWithSchema]](s"struct")
                var EXPOSURE = 1
                var DOWNLOAD_COMPLETE = 0
                struct.foreach(s => {
                    val behaviorType = s.getAs[String]("behaviorType")
                    val count = s.getAs[Long]("count").toInt
                    behaviorType match {
                        case "item_exposure" => EXPOSURE += count
                        case "item_click" => DOWNLOAD_COMPLETE += count
                    }
                })
                val ctr = wilsonCtrUDF(DOWNLOAD_COMPLETE, EXPOSURE)
                (r.getAs[String]("gaid"),
                    (r.getAs[Long]("intlCategoryId").toString, ctr))
            })
            .filter(r => r._2._2 > 0)
            .groupByKey()
            .map(row => {
                val id = row._1
                val hotTags: Map[String, Double] = row._2.toArray.sortBy(_._2).reverse.slice(0, 100).toMap
                (id, hotTags)
            })
            .toDF("id", "userHotTags")

        resultDf.show(false)
        resultDf.printSchema()
        resultDf.write.mode("overwrite").parquet(s"$dwd_appstore_recommend_user_offline_behavior_1d/locale=${locale2locale(locale)}/tag=userHotTag/date=$endDate")

    }
}
