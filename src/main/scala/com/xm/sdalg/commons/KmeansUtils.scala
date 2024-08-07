package com.xm.sdalg.commons

import com.xm.sdalg.commons.CalcUtils._
import org.apache.hadoop.fs._
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions.sum

import scala.collection.mutable.ArrayBuffer

/**
 * 聚类方法的抽象实现
 */
object KmeansUtils {
    /**
     * 计算每个节点的簇内聚合度和簇间分离度
     * 由于复杂度的问题，聚合度和分离度的计算方式和标准轮廓系数计算有所差别
     * 聚合度：计算给定节点与所在簇中心的距离（即第一小的距离
     * 分离度：计算给定节点与其他簇中心的最小距离（即第二小的距离
     */
    def itemDistanceWithCluster(sparkSession: SparkSession, orgDf: DataFrame, clusterCenter: Array[Array[Double]]): DataFrame = {
        import sparkSession.implicits._
        orgDf
            .rdd
            .map(r => (r.getAs[collection.mutable.WrappedArray[Double]]("sample_vec"), r.getAs[Long]("rank"), r.getAs[Int]("cluster")))
            .map(r => {
                val distacneArray = new ArrayBuffer[Double]()
                var cujian = 0.0 // 簇内凝聚度
                var cunei = 0.0 // 簇间分离度
                clusterCenter.indices.foreach(i => {
                    if (i == r._3) cunei = getEucDistance(r._1, clusterCenter(i))
                    else distacneArray.append(getEucDistance(r._1, clusterCenter(i)))
                })
                distacneArray.sorted
                cujian = distacneArray(0)
                val itemSihouette = (cujian - cunei) / Math.max(cujian, cunei) //计算给定item的轮廓系数
                (r._2, cunei, cujian, itemSihouette)
            })
            .toDF("rank", "cunei", "cujian", "itemSihouette") // id,簇内聚集度,簇间分离度,item轮廓系数
    }

    /**
     * 计算轮廓系数，来判断聚类效果好坏
     * 轮廓系数解释可见  https://www.cnblogs.com/wangleBlogs/p/10102492.html
     */
    def silhouetteCoefficient(sparkSession: SparkSession, model: KMeansModel, orgRdd: RDD[Vector]): Double = {
        import sparkSession.implicits._
        val clusterCenter: Array[Array[Double]] = model.clusterCenters.map(r => r.toArray)
        val rankDf = model.predict(orgRdd)
            .zipWithIndex()
            .toDF("cluster", "rank") // 簇中心，rank值，rank值用来做join
        val orgDf: DataFrame = orgRdd
            .map(r => r.toArray)
            .zipWithIndex()
            .toDF("sample_vec", "rank") // rank值用来做join
            .join(rankDf, Seq("rank"))
            .selectExpr("sample_vec", "cluster", "rank") // item向量，item簇，编号，用rank编号作为id
        val temp = itemDistanceWithCluster(sparkSession, orgDf, clusterCenter)
        val result = temp.agg(sum("itemSihouette")).map(_.getDouble(0)).collect().sum / temp.count().toDouble
        result
    }

    /**
     * 实现基于Kmeans的向量聚类
     *
     * @param orgRdd        原始的RDD数据，RDD[(String, Array[Double])],其中string是id，Array[Double]是向量
     * @param minK          最小的聚类簇数量
     * @param maxK          最大的聚类簇数量
     * @param modelSavePath 模型保存地址
     * @return id,Array[Double],cluster
     */
    def Kmeans(sparkSession: SparkSession,
               orgRdd: RDD[(String, Array[Double])],
               minK: Int,
               maxK: Int,
               modelSavePath: String,
               maxInterations: Int = 20,
               defaultCluster: Int = 0): DataFrame = {
        import sparkSession.implicits._
        val vecRdd: RDD[Vector] = orgRdd.map(r => Vectors.dense(r._2))
        var bestModel: KMeansModel = null
        var maxCoefficient = -1.0
        var bestK = 0
        Range(minK, maxK).foreach(k => {
            val KmeansModel: KMeansModel = new KMeans()
                .setK(k)
                .setInitializationMode("k-means||")
                .setMaxIterations(maxInterations)
                .run(vecRdd)
            val coefficient: Double = silhouetteCoefficient(sparkSession, KmeansModel, vecRdd)
            println(s"聚类簇个数为${k}时轮廓系数是${coefficient}")
            if (maxCoefficient < coefficient) {
                bestModel = KmeansModel
                maxCoefficient = coefficient
                bestK = k
            }
        })
        println(s"最佳聚类簇个数是${bestK}")
        val fs: FileSystem = new Path(modelSavePath).getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
        fs.delete(new Path(modelSavePath), true)
        bestModel.save(sparkSession.sparkContext, modelSavePath)

        val clusterDf = bestModel.predict(vecRdd).zipWithIndex().toDF("cluster", "rank")
        val idRankDf = orgRdd.zipWithIndex().map(r => (r._1._1, r._1._2, r._2)).toDF("product_id", "name_tag_emb", "rank")
        val resultDf = idRankDf.join(clusterDf, Seq("rank"), "left")
            .na.fill(defaultCluster)
            .selectExpr("product_id", "name_tag_emb", "cluster")
        resultDf
    }

}
