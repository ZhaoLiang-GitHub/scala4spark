package com.xm.sdalg.appstrore_recommend

import com.xm.sdalg.commons.SparkContextUtils.createSparkSession
import com.xm.sdalg.appstrore_recommend.Utils._
import com.xm.sdalg.commons.HDFSUtils._
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.xm.sdalg.commons.ClusterUtils._

/**
 * @author zhaoliang6 on 20220810
 *         用户离线行为join在一起，统一写入doc
 *         输入参数 locale,date,path1,path2,path3....
 *         每个path都是parquet格式的数据，以id列为主键，
 */
object UserBehaviorMerge {
    def main(args: Array[String]): Unit = {
        val Array(locale: String, date: String) = args.slice(0, 2)
        val pathArray = args.slice(2, args.length)
        val sparkSession: SparkSession = createSparkSession(this.getClass.getSimpleName)
        var resultDf: DataFrame = null
        pathArray.foreach(path => {
            println(path)
            val temp = sparkSession.read.parquet(s"${path}/date=$date")
                .where(filterGaid("id"))
                .dropDuplicates("id")
            if (resultDf == null) resultDf = temp
            else resultDf = resultDf.join(temp, Seq("id"), "outer")
        })

        resultDf.show(100, false)
        resultDf.printSchema()
        resultDf.write.mode("overwrite").parquet(s"$dwd_appstore_recommend_user_behavior_1d/locale=${locale2locale(locale)}/date=$date")
    }
}
