package com.xm.sdalg.browerSearch

import com.xm.sdalg.browerSearch.Utils.offlineScoreSet
import com.xm.sdalg.commons.HDFSUtils._
import com.xm.sdalg.commons.MysqlUtils._
import com.xm.sdalg.commons.SparkContextUtils._
import com.xm.sdalg.commons.TimeUtils._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.lit

object HotQuery2Iceberg {

    def main(args: Array[String]): Unit = {
        val sparkSession: SparkSession = createSparkSession(this.getClass.getSimpleName)
        import sparkSession.implicits._
        var orgDf = readFromMysql(sparkSession, hotSearchIp, hotSearchPort, hotSearchDataBase, hotSearchTable, hotSearchUser, hotSearchPassword)
            .selectExpr(
                "id",
                "name",
                "source",
                "image_url",
                "abstract",
                "type",
                "category",
                "position",
                "status",
                "create_time",
                "update_time",
                "show_title")
            .rdd
            .map(row => {
                val orgId = row.getAs[String]("id")
                val orgName = row.getAs[String]("name")
                val orgSource = row.getAs[String]("source")
                val orgImageUrl = row.getAs[String]("image_url")
                val orgAbstract = row.getAs[String]("abstract")
                val orgType = row.getAs[String]("type")
                val orgCategory = row.getAs[String]("category")
                val orgPosition = row.getAs[java.lang.Integer]("position").toString
                val orgStatus = row.getAs[java.lang.Integer]("status").toString
                val orgCreateTime = row.getAs[java.sql.Timestamp]("create_time").toString
                val orgUpdateTime = row.getAs[java.sql.Timestamp]("update_time").toString
                val orgShowTitle = row.getAs[String]("show_title")


                val id = ("bs_" + orgId).substring(0, 17)
                val title = "全网热搜"
                val subtitle = orgName
                val url = ""
                val createTimeDate = orgCreateTime.replaceAll("-", "").substring(0, 8)
                val updateTimeDate = orgUpdateTime.replaceAll("-", "").substring(0, 8)

                val availability = "browser"

                (
                    orgId, orgName, orgSource, orgImageUrl, orgAbstract, orgType, orgCategory, orgPosition, orgStatus, orgCreateTime, orgUpdateTime, orgShowTitle,
                    id, title, subtitle, url, createTimeDate, updateTimeDate,
                    availability
                )
            })
            .toDF("org_id",
                "org_name",
                "org_source",
                "org_image_url",
                "org_abstract",
                "org_type",
                "org_category",
                "org_position",
                "org_status",
                "org_create_time",
                "org_update_time",
                "org_show_title",
                "id",
                "title",
                "subtitle",
                "url",
                "create_time_date",
                "update_time_date",
                "availability"
            )
        val dwdBrowserSearchPushItemDf = sparkSession.table("iceberg_zjyprc_hadoop.browser.dwd_browser_search_push_item")
        orgDf = orgDf

            /**
             * 打分数据不是来自原始的表，所以需要从现有数据中进行join
             * 所以每次增删分数，都需要
             * 1. 修改表结构，增删分数特征
             * 2. 在 push-data-redis 这个仓库下ItemScore 类 修改写入特征
             * 3. 在当前类join上现有的特征
             * 4. 在写入表
             * 造成这个原因的问题是，这张表在两个地方读写，所以需要保持一致
             */
            .join(
                dwdBrowserSearchPushItemDf
                    .selectExpr((Array("id") ++ offlineScoreSet).toArray: _*),
                Seq("id"),
                "left"
            )
            .selectExpr(dwdBrowserSearchPushItemDf.schema.fieldNames: _*)
        orgDf.createOrReplaceTempView("temp")
        sparkSession.sql(
            """
              |insert
              |    overwrite
              |table
              |    iceberg_zjyprc_hadoop.browser.dwd_browser_search_push_item
              |select
              |     *
              |from
              |    temp
              |""".stripMargin)
        println(getCurrentDateStr(), "当前zeus数据库里的全部数据量级", orgDf.count())

    }
}
