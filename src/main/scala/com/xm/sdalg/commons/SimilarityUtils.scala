package com.xm.sdalg.commons

import com.xm.sdalg.commons.CalcUtils.getCosDistanceVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{HashingTF, IDF, IDFModel}
import org.apache.spark.ml.param.shared.HasNumFeatures
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.linalg.{Vector, Vectors}

import scala.collection.mutable

/**
 * 文本之间的相似度的计算方法
 */
object SimilarityUtils extends Serializable {
    /**
     * 两个字符串之间的编辑距离
     */
    def getLevenshteinDistance(word1: String, word2: String): Double = {
        val m = word1.length
        val n = word2.length
        if (m < n) {
            return getLevenshteinDistance(word2, word1)
        }
        val dp = Array.ofDim[Int](2, n + 1)
        for (j <- 0 to n) {
            dp(0)(j) = j
        }
        for (i <- 1 to m) {
            dp(i % 2)(0) = i
            for (j <- 1 to n) {
                val cost = if (word1(i - 1) == word2(j - 1)) 0 else 1
                dp(i % 2)(j) = Math.min(Math.min(dp((i - 1) % 2)(j) + 1, dp(i % 2)(j - 1) + 1), dp((i - 1) % 2)(j - 1) + cost)
            }
        }
        dp(m % 2)(n).toDouble
    }

    /**
     * 两个字符串之间的Jaccard距离
     */
    def getJaccardDistance(str1: String, str2: String, n: Int): Double = {
        // 辅助函数，生成一个字符串的N-grams集合
        def generateNGrams(text: String, n: Int): Set[String] = {
            val ngrams = mutable.Set[String]()
            for (i <- 0 until text.length - n + 1) {
                ngrams += text.substring(i, i + n)
            }
            ngrams.toSet
        }

        // 将输入字符串转换为小写
        val text1 = str1.replaceAll(" ", "").toLowerCase
        val text2 = str2.replaceAll(" ", "").toLowerCase

        // 生成两个字符串的N-grams集合
        val ngrams1 = generateNGrams(text1, n)
        val ngrams2 = generateNGrams(text2, n)

        // 计算交集和并集的大小
        val intersectionSize = ngrams1.intersect(ngrams2).size
        val unionSize = ngrams1.union(ngrams2).size

        // 计算Jaccard相似度
        if (unionSize == 0) {
            0.0 // 处理分母为零的情况
        } else {
            intersectionSize.toDouble / unionSize.toDouble
        }
    }

    def getTfidfSim(sparkSession: SparkSession,
                    rdd: RDD[(String, Array[String])],
                    numFeaturesOrg: Int = 0,
                    topN: Int = 100): RDD[(String, Map[String, Double])] = {
        import sparkSession.implicits._
        val numFeatures =
            if (numFeaturesOrg > 0)
                numFeaturesOrg
            else
                rdd.flatMap(_._2).distinct().count().toInt / 10
        val hashingTF: HashingTF = new HashingTF()
            .setInputCol("words")
            .setOutputCol("rawFeatures")
            .setNumFeatures(numFeatures)
        val idf = new IDF()
            .setInputCol("rawFeatures")
            .setOutputCol("features")

        val featurizedData: DataFrame = hashingTF.transform(rdd.toDF("id", "words"))
        val rescaledData: DataFrame = idf.fit(featurizedData).transform(featurizedData).cache()
        val res = rescaledData.as("df1")
            .crossJoin(rescaledData.as("df2"))
            .filter($"df1.id" =!= $"df2.id")
            .withColumn("similarity", getCosDistanceVector($"df1.features", $"df2.features"))
            .select($"df1.id".alias("id1"), $"df2.id".alias("id2"), $"similarity")
            .rdd
            .map(row => {
                val id1 = row.getAs[String]("id1")
                val id2 = row.getAs[String]("id2")
                val similarity = row.getAs[Double]("similarity")
                (id1, (id2, similarity))
            })
            .filter(_._2._2 > 0)
            .groupByKey()
            .map(row => {
                val id1 = row._1
                val maps = row._2.toArray.sortBy(_._2).reverse.slice(0, topN).toMap
                (id1, maps)
            })
        res
    }

}
