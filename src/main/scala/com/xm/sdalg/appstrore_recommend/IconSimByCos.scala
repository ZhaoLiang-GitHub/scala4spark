package com.xm.sdalg.appstrore_recommend

import com.xm.sdalg.appstrore_recommend.Utils._
import com.xm.sdalg.commons.CalcUtils._
import com.xm.sdalg.commons.ClusterUtils._
import com.xm.sdalg.commons.HDFSUtils._
import com.xm.sdalg.commons.SparkContextUtils._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._
import scala.collection.mutable.ArrayBuffer

object IconSimByCos {
    def main(args: Array[String]): Unit = {
        val sparkSession: SparkSession = createSparkSession(this.getClass.getSimpleName)
        val Array(locale, date) = args
        val allAppFilterSAB_CDV7: Broadcast[Set[String]] =
            if (localeInCluster(locale, singapore) && locale.equals("ID"))
                getAppSetBc(sparkSession, locale, date, "SAB", "CD")
            else
                getAppSetBc(sparkSession, locale, date, "SAB", "C")

        val df = sparkSession.read.parquet(s"/user/h_data_platform/platform/appstore/dwd_appstore_icon_vgg/date=$date")
            .filter(row => allAppFilterSAB_CDV7.value.contains(row.getAs[String]("pkg")))
            .dropDuplicates("pkg")
            .repartition(1000)

        val itemSimByVggVecDf = itemSimByVggVec(sparkSession, df, true)
        itemSimByVggVecDf.show(false)
        itemSimByVggVecDf.printSchema()
        dfSaveInAssignPath(sparkSession, "/user/h_sns/mipicks_recommend/app_icon/cosSim/result.csv", itemSimByVggVecDf, "csv")
    }

    def itemSimByVggVec(sparkSession: SparkSession, dataFrame: DataFrame, filter: Boolean = true): DataFrame = {
        import sparkSession.implicits._

        val df1 = dataFrame.selectExpr("pkg as pkg1", "pca as pca1")
        val df2 = dataFrame.selectExpr("pkg as pkg2", "pca as pca2")
        df1
            .crossJoin(df2)
            .where("pkg1 < pkg2")
            .rdd
            .map(row => (row.getAs[String]("pkg1"), row.getAs[String]("pkg2"), getCosDistance(row.getAs[Seq[Double]]("pca1"), row.getAs[Seq[Double]]("pca2"))))
            .flatMap(row => {
                val ab: ArrayBuffer[(String, (String, Double))] = ArrayBuffer()
                ab.append((row._1, (row._2, row._3)))
                ab.append((row._2, (row._1, row._3)))
                ab
            })
            .groupByKey()
            .map(row => {
                (row._1, row._2.toArray.sortBy(_._2).reverse.slice(0, 100).map(e => e._1 + "&" + e._2).mkString(","))
            })
            .toDF("pkg", "value")
    }
}
