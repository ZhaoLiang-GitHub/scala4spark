package com.xm.sdalg.commons

import com.google.common.hash.Hashing.murmur3_32
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import java.nio.charset.Charset
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object CalcUtils extends Serializable {

    /**
     * 保留n位小数，默认取两位小数.
     *
     * @param d
     * @param roundNum
     * @return
     */
    def getSampleDouble(d: Double, roundNum: Int = 100): Double = {
        math.round(d * roundNum) / (roundNum + 0.0)
    }

    /**
     * 计算比例,a/b
     *
     * @param a
     * @param b
     * @return
     */
    def getRate(a: Double, b: Double): Double = {
        if (b == 0.0) 0.0 else a / b
    }

    def getRateUDF: UserDefinedFunction = udf((a: Long, b: Long) => a.toDouble / b.toDouble)

    /**
     * 威尔逊系数计算ctr
     */
    def wilsonCtrUDF: UserDefinedFunction = udf((positive: Double, total: Double) => {
        if (total == 0)
            0.0
        else {
            val p = positive / total
            val z = 1.96
            val denominator = 1.0 + (z * z / total)
            val centreValue = p + (z * z / (2 * total))
            val marginOfError = z * math.sqrt(p * (1 - p) / total + z * z / (4 * total * total))
            (centreValue - marginOfError) / denominator
        }
    })

    def wilsonCtrUDF(positive: Double, total: Double): Double = {
        if (total == 0) return 0.0
        val p = positive / total
        val z = 1.96
        val denominator = 1.0 + (z * z / total)
        val centreValue = p + (z * z / (2 * total))
        val marginOfError = z * math.sqrt(p * (1 - p) / total + z * z / (4 * total * total))
        (centreValue - marginOfError) / denominator
    }

    // 两个向量的欧式距离，通过泛型来支持多种数据格式
    def getEucDistance[T](v1: Seq[T], v2: Seq[T])(implicit num: Numeric[T]): Double = {
        val sum = v1.zip(v2)
            .map { case (a, b) =>
                val diff = num.toDouble(num.minus(a, b))
                diff * diff
            }.sum
        math.sqrt(sum)
    }


    /**
     * 两个向量之间的cos距离
     */
    def getCosDistanceArray: UserDefinedFunction = udf((userEmb: mutable.WrappedArray[Double], itemEmb: mutable.WrappedArray[Double]) => {
        var cos = 0.0
        try {
            val uVecModule = math.sqrt(userEmb.map(math.pow(_, 2)).sum)
            val iVecModule = math.sqrt(itemEmb.map(math.pow(_, 2)).sum)
            val innerProduct = userEmb.zip(itemEmb).map(f => f._1 * f._2).sum
            cos = innerProduct / (uVecModule * iVecModule)
        } catch {
            case _: Exception =>
        }
        cos
    })

    def getCosDistanceVector: UserDefinedFunction = udf((vector1: Vector, vector2: Vector) => {
        val dotProduct = vector1.dot(vector2)
        val magnitude1 = Vectors.norm(vector1, 2.0) // L2范数
        val magnitude2 = Vectors.norm(vector2, 2.0)
        if (magnitude1 != 0.0 && magnitude2 != 0.0) {
            dotProduct / (magnitude1 * magnitude2)
        } else {
            0.0 // 避免除以零
        }
    })

    /**
     * 两个向量之间的cos距离，通过泛型来支持多种数据格式
     */
    def getCosDistance[T](v1: Seq[T], v2: Seq[T])(implicit num: Numeric[T]): Double = {
        val dotProduct = v1.zip(v2).map { case (a, b) => num.toDouble(num.times(a, b)) }.sum
        val norm1 = math.sqrt(v1.map(num.toDouble).map(a => a * a).sum)
        val norm2 = math.sqrt(v2.map(num.toDouble).map(a => a * a).sum)
        dotProduct / (norm1 * norm2)
    }

    def getCosDistance(vector1: Vector, vector2: Vector): Double = {
        val dotProduct = vector1.dot(vector2)
        val magnitude1 = Vectors.norm(vector1, 2.0) // L2范数
        val magnitude2 = Vectors.norm(vector2, 2.0)
        if (magnitude1 != 0.0 && magnitude2 != 0.0) {
            dotProduct / (magnitude1 * magnitude2)
        } else {
            0.0 // 避免除以零
        }
    }

    /**
     * 数组之间的组合结果，支持多种数据格式
     */
    def getCombinationResult[T](org: Set[T]): Set[(T, T)] = {
        org.flatMap(i => org.map(j => (i, j))).filter(f => f._1 != f._2)
    }

    /**
     * array[Double] 转 稠密向量
     */
    def array2vec(org: Array[Double]): Vector = Vectors.dense(org)

    /**
     * 将字符串在给定的key下做hash
     *
     * @param hashNum hash的key
     * @return
     */
    def hashStringUdf(hashNum: Int): UserDefinedFunction = {
        udf((col: String) => {
            (Math.abs(murmur3_32().hashString(col, Charset.defaultCharset()).asInt()) % hashNum).toDouble
        })
    }

    def stringSplitByPattern(s: String, pattern: Set[String]): Set[String] = {
        var result: Array[String] = null
        pattern.foreach(p => {
            if (result == null)
                result = s.split(p)
            else
                result = result.flatMap(_.split(p))
        })
        result.toSet
    }
}
