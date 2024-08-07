package com.xm.sdalg.appstrore_recommend

import com.xm.sdalg.appstrore_recommend.Utils._
import com.xm.sdalg.commons.ClusterUtils._
import com.xm.sdalg.commons.HDFSUtils._
import com.xm.sdalg.commons.SparkContextUtils._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._

/**
 * @author zhaoliang6 on 20220802
 *         从旧仓库中复制过来的热门数据的生成方式
 */
object HotItemInCate {
    def main(args: Array[String]): Unit = {
        val Array(locale: String, startDate: String, endDate: String) = args
        val sparkSession: SparkSession = createSparkSession(this.getClass.getSimpleName)
        var resultDf: Dataset[Row] = null
        var globalHotTag: DataFrame = null
        val package2idDf: DataFrame = package2intlCategoryId(sparkSession, locale)
            .selectExpr("package_name as packageName", "intl_category_id as intlCategoryId")
            .cache()
        if (locale == "ID") {
            // 20230726 第二版评级分在印尼推全
            val allAppFilterSAB_CDV9 = getAppSetBc(sparkSession, locale, endDate, "SAB", "CD")
            resultDf = union(resultDf, getHotItemInCate(sparkSession, locale, startDate, endDate, allAppFilterSAB_CDV9, name = "hotInCateSAB_CD_V9_", package2idDf))

            // 20231024 标签热门数据
            val package2tags = getPackage2Tags(sparkSession, locale).selectExpr("pkg as packageName", "tagid as intlCategoryId")
            resultDf = union(resultDf, getHotItemInCate(sparkSession, locale, startDate, endDate, allAppFilterSAB_CDV9, name = "hotInTag_", package2tags))
            resultDf = union(resultDf, getGlobalCateSort(sparkSession, locale, startDate, endDate, allAppFilterSAB_CDV9, name = "globalHotTag", package2tags))
        } else if (locale == "SG_OTHER") {
            val allAppFilterSAB_CV7 = getAppSetBc(sparkSession, locale, endDate, "SAB", "C")
            //仅土耳其，巴西两国需要召回
            resultDf =union(resultDf, getHotItemInCate(sparkSession, "TR", startDate, endDate, allAppFilterSAB_CV7, name= "hotInCateSAB_C_V7_TR_", package2idDf))
            resultDf =union(resultDf, getHotItemInCate(sparkSession, "BR", startDate, endDate, allAppFilterSAB_CV7, name= "hotInCateSAB_C_V7_BR_", package2idDf))
            // 20231114 其他国家标签热门数据
            val package2tags = getPackage2Tags(sparkSession, locale).selectExpr("pkg as packageName", "tagid as intlCategoryId")
            resultDf = union(resultDf, getHotItemInCate(sparkSession, "TR", startDate, endDate, allAppFilterSAB_CV7, name = "hotInTag_TR_", package2tags))
            resultDf = union(resultDf, getHotItemInCate(sparkSession, "BR", startDate, endDate, allAppFilterSAB_CV7, name = "hotInTag_BR_", package2tags))
            resultDf = union(resultDf, getGlobalCateSort(sparkSession, locale, startDate, endDate, allAppFilterSAB_CV7, name = "globalHotTag", package2tags))
        } else {
            val allAppFilterSAB_CV7 = getAppSetBc(sparkSession, locale, endDate, "SAB", "C")
            resultDf =union(resultDf, getHotItemInCate(sparkSession, locale, startDate, endDate, allAppFilterSAB_CV7, name = "hotInCateSAB_C_V7_", package2idDf))
            // 20231114 其他国家标签热门数据
            val package2tags = getPackage2Tags(sparkSession, locale).selectExpr("pkg as packageName", "tagid as intlCategoryId")
            resultDf = union(resultDf, getHotItemInCate(sparkSession, locale, startDate, endDate, allAppFilterSAB_CV7, name = "hotInTag_", package2tags))
            resultDf = union(resultDf, getGlobalCateSort(sparkSession, locale, startDate, endDate, allAppFilterSAB_CV7, name = "globalHotTag", package2tags))
        }

        resultDf.sort("id").show(1000, false)
        resultDf.printSchema()
        rddSaveInHDFS(sparkSession, resultDf.toJSON.rdd, dim_appstore_recommend_tag_doc(endDate, locale2locale(locale), "hotInCate"))
    }


    /**
     * 栏目下的热门结果
     * 基于下载人包判断热门结果
     */
    def getHotItemInCate(sparkSession: SparkSession,
                         locale: String,
                         startDate: String,
                         endDate: String,
                         whiteSet: Broadcast[Set[String]],
                         name: String,
                         package2idDf: DataFrame): DataFrame = {
        import sparkSession.implicits._
        readParquet4dwm_appstore_user_behavior_di(sparkSession, locale, startDate, endDate)
            .where(filterLocale("locale", locale))
            .where("id is not null and packageName is not null")
            .where("behaviorType in ('DOWNLOAD_COMPLETE','DOWNLOAD')")
            .selectExpr("id.id as id", "packageName", "date")
            .filter(row => whiteSet.value.contains(row.getAs[String]("packageName")))
            .distinct()
            .join(package2idDf, Seq("packageName"))
            .repartition(1000)
            .groupBy("intlCategoryId", "packageName").count()
            .selectExpr("region","intlCategoryId", "packageName", "count")
            .rdd
            .map(row => {
                val region = row.getString(-1)
                val intlCategoryId = row.getInt(0)
                val packageName = row.getString(1)
                val count = row.getLong(2).toDouble
                ((region,intlCategoryId), (packageName, count))
            })
            .groupByKey()
            .map(row => {
                val region = row._1._1
                val intlCategoryId = row._1._2
                (region, intlCategoryId, row._2.toArray.sortBy(_._2).reverse.slice(0, 200).toMap)
            })

            .toDF("id", "apps")
    }

    /**
     * 全局热门tag
     */
    def getGlobalCateSort(sparkSession: SparkSession,
                          locale: String,
                          startDate: String,
                          endDate: String,
                          whiteSet: Broadcast[Set[String]],
                          name: String,
                          package2idDf: DataFrame): DataFrame = {
        import sparkSession.implicits._
        readParquet4dwm_appstore_user_behavior_di(sparkSession, locale, startDate, endDate)
            .where(filterLocale("locale", locale))
            .where("id is not null and packageName is not null")
            .where("behaviorType in ('DOWNLOAD_COMPLETE','DOWNLOAD')")
            .selectExpr("id.id as id", "packageName", "date")
            .filter(row => whiteSet.value.contains(row.getAs[String]("packageName")))
            .distinct()
            .join(package2idDf, Seq("packageName"))
            .repartition(1000)
            .groupBy("intlCategoryId").count()
            .rdd
            .map(row => {
                val intlCategoryId = row.getInt(0)
                val count = row.getLong(1).toDouble
                (name, (intlCategoryId, count))
            })
            .groupByKey()
            .map(row => {
                val id = row._1
                val apps: Map[Int, Double] = row._2.toArray.sortBy(_._2).reverse.slice(0, 100).toMap
                (id, apps)
            })
            .toDF("id", "apps")
    }
}
