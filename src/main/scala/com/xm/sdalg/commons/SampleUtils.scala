package com.xm.sdalg.commons

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object SampleUtils extends Serializable {
    /**
     * 别名采样法 https://blog.csdn.net/haolexiao/article/details/65157026
     *
     * @example
     * val alias: Alias = new Alias(Array[(Long,Double)]).setAlias
     * for (i <- 0 to 10) println(alias.sample)
     * @param sampleProbability 样本id及对应的概率，概率可以不做归一化
     *                          构建对于每个样本（位置）而言的最大采样概率与拒绝采样时的别名
     */
    class Alias(sampleProbability: Array[(Long, Double)]) extends Serializable {
        var other: Array[Int] = _ // 选中该位置时，随机数大于该位置最大概率时的选择，即别名
        var maxPro: Array[Double] = _ // 选中该位置时取对应值的最大概率
        var sampleIndex: Array[Long] = _ // 所有的样本id

        def setAlias: this.type = {
            val smaller = new ArrayBuffer[Int]() // 归一化概率小于1的样本下标
            val larger = new ArrayBuffer[Int]()
            val sampleNum = sampleProbability.length
            sampleIndex = sampleProbability.map(_._1)
            other = Array.fill(sampleNum)(0)
            maxPro = Array.fill(sampleNum)(0.0)

            val allPro = sampleProbability.map(_._2).sum
            sampleProbability
                .zipWithIndex
                .foreach({ case ((nodeId, pro), i) =>
                    maxPro(i) = pro / (allPro / sampleNum) // 对均值做归一化
                    if (maxPro(i) < 1.0)
                        smaller.append(i)
                    else
                        larger.append(i)
                })
            while (smaller.nonEmpty && larger.nonEmpty) {
                val small: Int = smaller.remove(0)
                val large: Int = larger.remove(0)
                other(small) = large
                maxPro(large) = maxPro(large) + maxPro(small) - 1.0
                if (maxPro(large) < 1.0)
                    smaller.append(large)
                else if (maxPro(large) > 1.0)
                    larger.append(large)
            }
            this
        }

        /**
         * 随机采样，采样结果与原概率保持一致
         * 返回值为样本的id
         */
        def sample: Long = sampleIndex(sampleIndexOfIter)

        /**
         * 通过采样，获得下一个要采样的索引
         */
        def sampleIndexOfIter: Int = {
            val num = other.length
            val index = math.floor(math.random * num).toInt
            if (math.random < maxPro(index))
                index
            else
                other(index)
        }
    }

    /**
     * 对输入的array[(Long,Double)]进行随机采样
     */
    class Random(sampleProbability: Array[(Long, Double)]) extends Serializable {
        def sample(topK: Int): Array[(Long, Double)] = {
            if (sampleProbability.length <= topK)
                sampleProbability
            else
                Random
                    .shuffle(sampleProbability.toSeq)
                    .take(topK)
                    .toArray
        }
    }
}
