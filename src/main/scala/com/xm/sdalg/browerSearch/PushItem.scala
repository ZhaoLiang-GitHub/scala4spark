package com.xm.sdalg.browerSearch

import com.xm.sdalg.browerSearch.Utils.offlineScoreSet
import com.xm.sdalg.commons.HDFSUtils._
import com.xm.sdalg.commons.SparkContextUtils._
import com.xm.sdalg.commons.TimeUtils._
import org.apache.spark.sql._

object PushItem {
    val topK = 30 // 离线打分的前多少个被审核

    def main(args: Array[String]): Unit = {
        val sparkSession: SparkSession = createSparkSession(this.getClass.getSimpleName)
        val sql =
            s"""
               |select *,
               |${offlineScoreSet.map(i => s"rank() over(order by ${i} desc) as ${i}_rank").mkString(",")}
               |from iceberg_zjyprc_hadoop.browser.dwd_browser_search_push_item
               |where
               |org_source = 'baidu'
               |and create_time_date = '${getCurrentDateStr()}'
               |having
               |${offlineScoreSet.map(i => s"${i}_rank < ${topK}").mkString(" or ")}
               |""".stripMargin
        println(s"执行的sql是 \n ${sql}")
        val res = sparkSession.sql(sql)
            .selectExpr("id", "title", "subtitle", "url", "availability")
        res.show(100000, false)
        println(s"候选集大小${res.count()}")
        rddSaveInHDFS(sparkSession, res.toJSON.rdd, s"/user/h_browser/gaozhenzhuo/pushWork/aigp/hotquery/date=${getCurrentDateStr()}")


    }
}
