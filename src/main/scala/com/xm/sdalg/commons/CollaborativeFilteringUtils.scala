package com.xm.sdalg.commons


import com.linkedin.nn.distance.Distance
import com.xm.sdalg.commons.CalcUtils._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.recommendation._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer
import scala.math.{pow, sqrt}
import scala.util.Random

/**
 * @author zhaoliang6 on 20220818
 *         有关协同过滤相关算法的抽象实现 https://toutiao.io/posts/lfzqdf9/preview
 */
object CollaborativeFilteringUtils extends Serializable {

    /**
     * 基于item之间的相似度，得到最相似的前N个结果
     *
     * @param orgDf        item1 item2 similarity
     * @param sign         排序方式，默认是1，即采用cos距离按照相似度越大越相似，如果采用欧式距离等越小越相似则该值为-1
     * @param topN         每个item取最相似的前topN个
     * @param minSimScore  最小的相似度分数，默认不做过滤
     * @param whiteItemSet i2i右边的i可推荐的内容，如果为null，则全部可推荐
     * @param blackItemSet i2i右边的i不可以推荐的内容，如果为null，则全部可推荐
     * @param isDirected   是否是有向的，即只计算一遍
     * @return 当前的id,和最相似的前topN个结果与对应的相似度
     */
    def getI2IMapBySim(orgDf: DataFrame,
                       sign: Int = 1,
                       topN: Int = 100,
                       minSimScore: Double = -100000000D,
                       whiteItemSet: Broadcast[Set[String]] = null,
                       blackItemSet: Broadcast[Set[String]] = null,
                       isDirected: Boolean = false,
                       window: Int = 100): RDD[(String, Map[String, Double])] = {
        orgDf
            .rdd
            .flatMap(r =>
                if (isDirected)
                    Array((r.getString(0) + "&" + Random.nextInt(window), (r.getString(1), sign * r.getDouble(2))))
                else
                // AB BA 都需要计算相似度
                    Array((r.getString(0) + "&" + Random.nextInt(window), (r.getString(1), sign * r.getDouble(2))),
                        (r.getString(1) + "&" + Random.nextInt(window), (r.getString(0), sign * r.getDouble(2)))))
            .filter(_._2._2 >= minSimScore)
            .filter(row => {
                var flag = true
                if (whiteItemSet != null)
                    flag = whiteItemSet.value.contains(row._2._1)
                flag
            })
            .filter(row => {
                var flag = true
                if (blackItemSet != null)
                    flag = !blackItemSet.value.contains(row._2._1)
                flag
            })
            .groupByKey()
            .map(row => {
                val id = row._1
                val map: Array[(String, Double)] = row._2.toArray.sortBy(_._2).reverse.slice(0, topN)
                (id.split("&")(0), map)
            })
            .groupByKey()
            .map(row => {
                val id = row._1
                val map: Map[String, Double] = row._2.flatten.toArray.sortBy(_._2).reverse.slice(0, topN).toMap
                (id, map)
            })
    }

    /**
     * 基于item的向量计算得到i2i映射结果
     *
     * @param orgRdd       itemId,与对应的向量
     * @param distanceType 基于向量计算相似度的方式
     *                     其他参数与 getI2IMapBySim 函数含义相同
     */
    def getI2IMapByVec(sparkSession: SparkSession,
                       orgRdd: RDD[(String, Array[Double])],
                       distanceType: String = "cos",
                       topN: Int = 100,
                       minSimScore: Double = -10000D,
                       whiteItemSet: Broadcast[Set[String]],
                       blackItemSet: Broadcast[Set[String]] = null
                      ): RDD[(String, Map[String, Double])] = {
        import sparkSession.implicits._
        var sign = 1
        val orgDf = orgRdd.repartition(1000)
            .cartesian(orgRdd.repartition(1000))
            .filter(row => row._1._1 < row._2._1) // AB BA只计算一次相似度
            .filter(row => whiteItemSet.value.contains(row._1._1) || whiteItemSet.value.contains(row._2._1))
            .map(row => {
                val distance = distanceType match {
                    case "cos" =>
                        sign = 1
                        getCosDistance(row._1._2, row._2._2)
                    case "euc" =>
                        sign = -1
                        getEucDistance(row._1._2, row._2._2)
                }
                (row._1._1, row._2._1, distance)
            })
            .filter(_._3 >= minSimScore)
            .toDF("item1", "item2", "distance")
        getI2IMapBySim(orgDf, sign, topN, minSimScore, whiteItemSet, blackItemSet)
    }

    /**
     * 直接计算全部结果与候选集之间的相似度
     * 其他参数与 getI2IMapByVec 含义相同
     * */
    def getI2IMapByVecDirected(sparkSession: SparkSession,
                               orgRdd: RDD[(String, Array[Double])],
                               distanceType: String = "cos",
                               topN: Int = 100,
                               minSimScore: Double = -100000D,
                               whiteItemSet: Broadcast[Set[String]] = null,
                               blackItemSet: Broadcast[Set[String]] = null): RDD[(String, Map[String, Double])] = {
        import sparkSession.implicits._
        val candidate = orgRdd
            .filter(row => {
                var flag = true
                if (whiteItemSet != null)
                    flag = whiteItemSet.value.contains(row._1)
                flag
            })
            .filter(row => {
                var flag = true
                if (blackItemSet != null)
                    flag = !blackItemSet.value.contains(row._1)
                flag
            })
        var sign = 1
        val distanceDf = orgRdd.repartition(1000)
            .cartesian(candidate.repartition(1000))
            .map(row => {
                val distance = distanceType match {
                    case "cos" =>
                        sign = 1
                        getCosDistance(row._1._2, row._2._2)
                    case "euc" =>
                        sign = -1
                        getEucDistance(row._1._2, row._2._2)
                }
                (row._1._1, row._2._1, distance)
            })
            .filter(_._3 >= minSimScore)
            .toDF("item1", "item2", "distance")
        getI2IMapBySim(distanceDf, sign, topN, minSimScore, whiteItemSet, blackItemSet, isDirected = true)
    }

    /**
     * 通过 ScANNs 的相似最邻近工具找到每个元素的topN
     * https://www.jianshu.com/p/b72af2d27235
     */
    @deprecated("效果与faiss差距太大，不在使用")
    def getI2IMapByVecByScANNs(sparkSession: SparkSession,
                               orgRdd: RDD[(String, Array[Double])],
                               topN: Int = 100,
                               whiteItemSet: Broadcast[Set[String]] = null,
                               blackItemSet: Broadcast[Set[String]] = null,
                               numHashes: Int = 1000,
                               signatureLength: Int = 1000,
                               joinParallelism: Int = 5000,
                               bucketLimit: Int = 1000,
                               shouldSampleBuckets: Boolean = true,
                               numOutputPartitions: Int = 1000,
                               minScore: Double = -10000D): RDD[(String, Map[String, Double])] = {
        import com.linkedin.nn.algorithm._
        import sparkSession.implicits._
        val string2long: RDD[(String, Long)] = orgRdd
            .map(_._1)
            .distinct()
            .zipWithIndex()
        val string2longDf = string2long
            .map(row => (row._2, row._1))
            .toDF("long", "string")
        val orgVec: RDD[(Long, Vector)] = orgRdd
            .join(string2long)
            .map(row => (row._2._2, array2vec(row._2._1)))
        val candidate: RDD[(Long, Vector)] = orgRdd
            .filter(row => {
                var flag = true
                if (whiteItemSet != null)
                    flag = whiteItemSet.value.contains(row._1)
                flag
            })
            .filter(row => {
                var flag = true
                if (blackItemSet != null)
                    flag = !blackItemSet.value.contains(row._1)
                flag
            })
            .join(string2long)
            .map(row => (row._2._2, array2vec(row._2._1)))

        val model = new CosineSignRandomProjectionNNS() // cos 距离，越大越相似
            .setNumHashes(numHashes)
            .setSignatureLength(signatureLength)
            .setJoinParallelism(joinParallelism)
            .setBucketLimit(bucketLimit)
            .setShouldSampleBuckets(shouldSampleBuckets)
            .setNumOutputPartitions(numOutputPartitions)
            .createModel(orgRdd.first._2.length)

        val result: RDD[(String, Map[String, Double])] = model.getAllNearestNeighbors(orgVec, candidate, topN)
            .map(row => (row._1, row._2, row._3))
            .toDF("long1", "long2", "sim")
            .join(string2longDf.selectExpr("long as long1", "string as string1"), Seq("long1"))
            .join(string2longDf.selectExpr("long as long2", "string as string2"), Seq("long2"))
            .selectExpr("string1", "string2", "sim")
            .where(s"sim > $minScore")
            .rdd
            .map(row => (row.getString(0), (row.getString(1), row.getDouble(2))))
            .groupByKey()
            .map(row => (row._1, row._2.toMap))
        println(s"全部可推荐内容池大小 ${candidate.count()}")
        println(s"i2i左边可用的用户行为数量是 ${orgVec.count()}")
        println(
            s"""
               |i2i右边可召回的结果
               |最多有 ${result.map(_._2.size).max()}
               |最少有 ${result.map(_._2.size).min()}
               |平均有 ${result.map(_._2.size).sum() / orgVec.count()}
               |""".stripMargin)
        result
    }

    /**
     * 对已经生成的i2i的结果做统计分析
     *
     * @param orgDf 最后的结果，结构是以key为主键，value 是 Map[String,Double]
     */
    def metric4res(orgDf: DataFrame,
                   key: String,
                   value: String,
                   topN: Int = 5): Unit = {
        val orgRdd = orgDf
            .selectExpr(key, value)
            .rdd
            .map(row => {
                val id = row.getString(0)
                val all = new ArrayBuffer[(String, Double)]()
                if (!row.isNullAt(1))
                    row.getMap[String, Double](1).foreach(i => all.append((i._1, i._2)))
                (id, all)
            })
        println(
            s"""
               |$value 方式的统计结果如下：
               |    有${orgRdd.map(_._1).distinct().count()}个item可以进行i2i召回
               |    一个item最多有${orgRdd.map(_._2.length).max()}个可召回结果，最少有${orgRdd.map(_._2.length).min()}个结果
               |    两个item之间最大的相似度是${orgRdd.flatMap(_._2.map(_._2)).max()}，最小的是${orgRdd.flatMap(_._2.map(_._2)).min()}
               |    通过i2i被召回次数最多的前${topN}的item及次数是${orgRdd.flatMap(_._2.toArray.map(i => (i._1, 1))).reduceByKey(_ + _).collect().sortBy(_._2).reverse.slice(0, topN).mkString(",")}
               |    通过i2i被召回次数最少的前${topN}的item及次数是${orgRdd.flatMap(_._2.toArray.map(i => (i._1, 1))).reduceByKey(_ + _).collect().sortBy(_._2).slice(0, topN).mkString(",")}
               |""".stripMargin)
    }

    /**
     * 对i2i输入的数据做数据统计
     *
     * @param input   计算i2i的原始日志输入，为了方便计算，需要将列名统一成user、item、score
     * @param output  i2i的输出结果，为item1、item2、sim,为防止AB BA只计算一遍的情况，所以在这里需要拆开
     * @param i2iType 当前i2i计算方式
     */
    def metrics4input(sparkSession: SparkSession,
                      input: DataFrame,
                      output: DataFrame,
                      i2iType: String): Unit = {
        println(s"当前i2i的计算方式是${i2iType}")
        import sparkSession.implicits._
        println(s"当前日志中共有${input.selectExpr("user").distinct().count()}个user")
        println(s"当前日志中共有${input.selectExpr("item").distinct().count()}个item")
        val temp1 = output
            .rdd
            .flatMap(row => Array(
                (row.getString(0), row.getString(1), row.getDouble(2)),
                (row.getString(1), row.getString(0), row.getDouble(2))
            ))
            .toDF("item1", "item2", "sim")
        println(s"有${temp1.selectExpr("item1").distinct().count()}个item可以进行i2i召回")
        val temp2 = temp1.groupBy("item1").agg(countDistinct("item2").as("count"), min("sim").as("minSim"), max("sim").as("maxSim"))
        val maxMinItem2: Array[(Long, Long)] = temp2.agg(max("count"), min("count"))
            .collect()
            .map(row => (row.getLong(0), row.getLong(1)))
        println(s"一个item最多有${maxMinItem2(0)._1}个可召回结果，最少有${maxMinItem2(0)._2}个结果")
        val maxMinSim: Array[(Double, Double)] = temp2.agg(max("maxSim"), min("minSim"))
            .collect()
            .map(row => (row.getDouble(0), row.getDouble(1)))
        println(s"两个item之间最大的相似度是${maxMinSim(0)._1}，最小的是${maxMinSim(0)._2}\n")
    }


    /**
     * 基于itemCF计算item之间的相似度，u2u类似
     *
     * @param orgDf   user-item-score 矩阵
     * @param version 不同的计算版本，默认为V1即原始的itemCF计算方法
     * @return
     */
    def itemCF(sparkSession: SparkSession,
               orgDf: DataFrame,
               version: String = "V1",
               verbose: Boolean = false): DataFrame = {
        import sparkSession.implicits._
        val multiply2ColUDF: UserDefinedFunction = udf((a: Double, b: Double) => a * b)
        val divide2ColUDF: UserDefinedFunction = udf((a: Double, b: Double, c: Double) => a / (b * c))
        val divide1ColUDF: UserDefinedFunction = udf((a: Double, b: Double, c: Double) => a * b / math.sqrt(c))
        val column = orgDf.columns
        val userItemScore = orgDf
            .selectExpr(s"cast(${column(0)} as string) as user", s"cast(${column(1)} as string) as item", s"cast(${column(2)} as double) as score")
            .cache()

        // item的向量模长
        val itemLength = userItemScore
            .rdd
            .map(row => (row.getString(1), row.getDouble(2)))
            .filter(row => row._2 != 0)
            .groupByKey()
            .mapValues(x => sqrt(x.toArray.map(ii => pow(ii, 2)).sum))
            .toDF("item", "length")

        var resultDf: DataFrame = null
        version match {
            case "V1" => {
                // 计算cos距离的分子
                val itemPairNumerator = userItemScore.selectExpr("user", "item as item1", "score as score1")
                    .join(userItemScore.selectExpr("user", "item as item2", "score as score2"), Seq("user"))
                    // 只要当一个用户V对两个商品都有行为时，在计算cos距离时，在第V个位置上才会计算出非0
                    .where("item1 < item2")
                    .withColumn("temp", multiply2ColUDF(col("score1"), col("score2")))
                    .groupBy("item1", "item2").agg(sum("temp").as("numerator"))
                resultDf = itemPairNumerator
                    .join(itemLength.selectExpr("item as item1", "length as length1"), Seq("item1"))
                    .join(itemLength.selectExpr("item as item2", "length as length2"), Seq("item2"))
                    // 两个item之间的相似度，即用全局用户组成的向量之间的cos距离,cos距离见 https://blog.csdn.net/m0_37192554/article/details/107359291
                    .withColumn("sim", divide2ColUDF(col("numerator"), col("length1"), col("length2")))
                    .select("item1", "item2", "sim")
            }
            case "V2" => {
                // 结合swing的方法，在计算两个item之间相似度时，对重度用户做了惩罚
                val userItemSet = userItemScore
                    .rdd
                    .map(row => (row.getAs[String]("user"), row.getAs[String]("item")))
                    .groupByKey()
                    .map(row => (row._1, row._2.toSet.size))
                    .toDF("user", "setSize") // user，userSetSize
                    .where("setSize > 0")
                    .repartition(1000)
                val itemPairNumerator = userItemScore.selectExpr("user", "item as item1", "score as score1")
                    .join(userItemScore.selectExpr("user", "item as item2", "score as score2"), Seq("user"))
                    .join(userItemSet, Seq("user"))
                    // 只要当一个用户V对两个商品都有行为时，在计算cos距离时，在第V个位置上才会计算出非0
                    .where("item1 < item2")
                    .withColumn("temp", divide1ColUDF(col("score1"), col("score2"), col("setSize")))
                    .groupBy(s"item1", s"item2").agg(sum("temp").as("numerator"))
                resultDf = itemPairNumerator
                    .join(itemLength.selectExpr("item as item1", "length as length1"), Seq("item1"))
                    .join(itemLength.selectExpr("item as item2", "length as length2"), Seq("item2"))
                    // 两个item之间的相似度，即用全局用户组成的向量之间的cos距离,cos距离见 https://blog.csdn.net/m0_37192554/article/details/107359291
                    .withColumn("sim", divide2ColUDF(col("numerator"), col("length1"), col("length2")))
                    .select("item1", s"item2", "sim")
            }
        }
        if (verbose) metrics4input(sparkSession, userItemScore, resultDf, s"itemCf_$version")
        resultDf
    }


    /**
     * 基于swing计算item之间的相似度
     * swing的计算方式可见 https://blog.csdn.net/qq_33534428/article/details/124839278
     *
     * @param orgDf   user-item-score 当前user对这个item的得分
     * @param alpha   平滑系数，防止分母为0
     * @param version 不同的相似度计算方法
     * @param verbose 是否展示统计日志
     * @return item1,item2,sim
     */
    def swing(sparkSession: SparkSession,
              orgDf: DataFrame,
              alpha: Int = 1,
              version: String = "V1",
              verbose: Boolean = false,
              userMinItemCount: Long = -1,
              userMaxItemCount: Long = 100000000,
              itemMinUserCount: Long = -1,
              itemMaxUserCount: Long = 100000000,
              @deprecated minCoUser: Long = 0,
              @deprecated isSampling: Boolean = false): DataFrame = {
        import sparkSession.implicits._
        val joinStringUDF: UserDefinedFunction = udf((a: String, b: String) => a + "_" + b)
        val joinDoubleUDF: UserDefinedFunction = udf((a: Double, b: Double) => a.toString + "_" + b.toString)

        def splitDouble(s: String): Array[Double] = s.split("_").map(_.toDouble)

        val column = orgDf.columns
        var userItemScore = orgDf
            .selectExpr(s"cast(${column(0)} as string) as user", s"cast(${column(1)} as string) as item", s"cast(${column(2)} as double) as score") // user,item,score
            .repartition(1000)

        val itemFilter = userItemScore
            .groupBy("item").count()
            .where(s"count >= $itemMinUserCount and count <= $itemMaxUserCount")
            .selectExpr("item")
        val userFilter = userItemScore
            .groupBy("user").count()
            .where(s"count >= $userMinItemCount and count <= $userMaxItemCount")
            .selectExpr("user")
        userItemScore = userItemScore
            .join(itemFilter, Seq("item"))
            .join(userFilter, Seq("user"))
            .repartition(1000)

        val itemLength = userItemScore
            .selectExpr("user", "item", "score")
            .rdd
            .map(row => (row.getAs[String]("item"), pow(row.getAs[Double]("score"), 2)))
            .reduceByKey(_ + _)
            .mapValues(sqrt)
            .toDF("item", "length")

        val itemPairUser = userItemScore.selectExpr("item as item1", "user", "score as score1")
            .join(userItemScore.selectExpr("item as item2", "user", "score as score2"), Seq("user"))
            .where("item1 < item2") // AB BA 只要一次
            .join(itemLength.selectExpr("item as item1", "length as length1"), Seq("item1"))
            .join(itemLength.selectExpr("item as item2", "length as length2"), Seq("item2"))
            .withColumn("itemPair", joinStringUDF(col("item1"), col("item2")))
            .withColumn("scorePair", joinDoubleUDF(col("score1"), col("score2")))
            .withColumn("lengthPair", joinDoubleUDF(col("length1"), col("length2")))
            .selectExpr("user", "itemPair", "scorePair", "lengthPair") // user对两个item都有行为，其中scorePair是两个行为的分数
            .distinct()

        val itemPairUserPair = itemPairUser.selectExpr("itemPair", "user as user1", "scorePair as scorePair1", "lengthPair")
            .join(itemPairUser.selectExpr("itemPair", "user as user2", "scorePair as scorePair2", "lengthPair"), Seq("itemPair"))
            .where("user1 <= user2")
            .withColumn("userPair", joinStringUDF(col("user1"), col("user2")))

        var resultDf: DataFrame = null
        version match {
            case "V1" => {
                resultDf = itemPairUserPair
                    // userPair,itemPair,setSizePair,scorePair1,scorePair2
                    // 这两个用户，对于这两个item都有过正向行为，构成了一个swing结构，其中setSizePair是这两个用户的item集合大小，scorePair1是用户1对两个item的行为分
                    .rdd
                    .map(row => (row.getAs[String]("userPair"), row.getAs[String]("itemPair")))
                    .repartition(10000)
                    .groupByKey()
                    .filter(row => row._2.nonEmpty)
                    .flatMap(row => {
                        val swing = 1D / (alpha + row._2.size).toDouble // 基础swing分
                        row._2.map(ii => (ii, swing))
                    })
                    .repartition(10000)
                    .reduceByKey(_ + _) // 对于两个item而言，将所有的swing得分求和，即是两者之间的相似度
                    .map(row => {
                        val temp = row._1.split("_")
                        (temp(0), temp(1), row._2)
                    })
                    .toDF("item1", "item2", "sim")
            }
            case "V1p1" => {
                resultDf = itemPairUserPair
                    .rdd
                    .map(row =>
                        (
                            (row.getAs[String]("userPair")),
                            (row.getAs[String]("itemPair"), row.getAs[String]("scorePair1"), row.getAs[String]("scorePair2"), row.getAs[String]("lengthPair"))
                        )
                    )
                    .repartition(10000)
                    .groupByKey()
                    .filter(row => row._2.nonEmpty)
                    .flatMap(row => {
                        row._2.map(ii => {
                            val Array(user1Item1Score, user1Item2Score) = splitDouble(ii._2)
                            val Array(user2Item1Score, user2Item2Score) = splitDouble(ii._3)
                            val Array(length1, length2) = splitDouble(ii._4)
                            val swing = 1D / (alpha + row._2.size).toDouble // 基础swing分
                            val S_uvij = (user1Item1Score * user2Item1Score + user1Item2Score * user2Item2Score) / math.sqrt(length1 * length2)
                            (ii._1, S_uvij * swing)
                        })
                    })
                    .repartition(10000)
                    .reduceByKey(_ + _) // 对于两个item而言，将所有的swing得分求和，即是两者之间的相似度
                    .map(row => {
                        val temp = row._1.split("_")
                        (temp(0), temp(1), row._2)
                    })
                    .toDF("item1", "item2", "sim")
            }
            case "V2" => {
                val userItemSet = userItemScore
                    .rdd
                    .map(row => (row.getAs[String]("user"), row.getAs[String]("item")))
                    .groupByKey()
                    .map(row => (row._1, row._2.toSet.size))
                    .toDF("user", "setSize") // user，userSetSize
                    .where("setSize > 0")
                    .repartition(1000)
                resultDf = itemPairUserPair
                    .join(userItemSet.selectExpr("user as user1", "setSize as setSize1"), Seq("user1"))
                    .join(userItemSet.selectExpr("user as user2", "setSize as setSize2"), Seq("user2"))
                    .withColumn("setSizePair", joinDoubleUDF(col("setSize1"), col("setSize2")))
                    .rdd
                    .map(row => ((row.getAs[String]("userPair"), row.getAs[String]("setSizePair")), row.getAs[String]("itemPair")))
                    .repartition(10000)
                    .groupByKey()
                    .filter(row => row._2.nonEmpty)
                    .flatMap(row => {
                        val Array(setSize1, setSize2) = splitDouble(row._1._2)
                        val W_uv = (1D / math.sqrt(setSize1)) * (1D / math.sqrt(setSize2)) // 用户权重
                        val swing = 1D / (alpha + row._2.size).toDouble // 基础swing分
                        row._2.map(ii => (ii, W_uv * swing))
                    })
                    .repartition(10000)
                    .reduceByKey(_ + _) // 对于两个item而言，将所有的swing得分求和，即是两者之间的相似度
                    .map(row => {
                        val temp = row._1.split("_")
                        (temp(0), temp(1), row._2)
                    })
                    .toDF("item1", "item2", "sim")
            }
            case "V3" => {
                val userItemSet = userItemScore
                    .rdd
                    .map(row => (row.getAs[String]("user"), row.getAs[String]("item")))
                    .groupByKey()
                    .map(row => (row._1, row._2.toSet.size))
                    .toDF("user", "setSize") // user，userSetSize
                    .where("setSize > 0")
                    .repartition(1000)
                resultDf = itemPairUserPair
                    .join(userItemSet.selectExpr("user as user1", "setSize as setSize1"), Seq("user1"))
                    .join(userItemSet.selectExpr("user as user2", "setSize as setSize2"), Seq("user2"))
                    .withColumn("setSizePair", joinDoubleUDF(col("setSize1"), col("setSize2")))
                    .rdd
                    .map(row =>
                        (
                            (row.getAs[String]("userPair"), row.getAs[String]("setSizePair")),
                            (row.getAs[String]("itemPair"), row.getAs[String]("scorePair1"), row.getAs[String]("scorePair2"), row.getAs[String]("lengthPair"))
                        )
                    )
                    .repartition(10000)
                    .groupByKey()
                    .filter(row => row._2.nonEmpty)
                    .flatMap(row => {
                        val Array(setSize1, setSize2) = splitDouble(row._1._2)
                        row._2.map(ii => {
                            val Array(user1Item1Score, user1Item2Score) = splitDouble(ii._2)
                            val Array(user2Item1Score, user2Item2Score) = splitDouble(ii._3)
                            val Array(length1, length2) = splitDouble(ii._4)
                            val W_uv = (1D / math.sqrt(setSize1)) * (1D / math.sqrt(setSize2)) // 用户权重
                            val swing = 1D / (alpha + row._2.size).toDouble // 基础swing分
                            /**
                             * @note 在加入了user-item-score之后，实际上swing就是将itemcf中两个item只计算一次的相似度，拆解成了加权后的多次
                             *       如果N个用户对两个item都有过行为的话，itemCF只需要将整体计算1次即可，对于同一个用户，只计算一次分子不会与其他用户产生关系
                             *       swing需要计算 C_N^^2 个用户对儿，计算这每个用户对儿之间的相似度然后再求和
                             *       相较于itemcf，增加了ijuv的swing分数与用户权重，在itemcf的基础上对这两个用户行为的分做了符合认知的惩罚
                             *       当单个用户行为更多时对item之间相似度共现越小，当两个用户共同行为越多时item之间相似性越小
                             */
                            val S_uvij = (user1Item1Score * user1Item2Score + user2Item1Score * user2Item2Score) / math.sqrt(length1 * length2)
                            (ii._1, S_uvij * W_uv * swing)
                        })
                    })
                    .reduceByKey(_ + _) // 对于两个item而言，将所有的swing得分求和，即是两者之间的相似度
                    .map(row => {
                        val temp = row._1.split("_")
                        (temp(0), temp(1), row._2)
                    })
                    .toDF("item1", "item2", "sim")
            }
        }
        if (verbose) metrics4input(sparkSession, userItemScore, resultDf, s"swing_$version")
        resultDf
    }


    /**
     * 对原始的user-item-score 矩阵通过ALS算法实现矩阵分解
     * 找到每个user最相似的recommendNum个item
     *
     * @param orgDf         原始的user-item-score矩阵，共有三列
     * @param userFeature   orgDf中的user列的列名，在ALS中user需要用int表示，需要对原始的内容做hash得到int
     * @param itemFeature   orgDf中的item列的列名，作用同上
     * @param ratingFeature orgDf中的user-item 相关性得分列的列名
     * @param recommendNum  每个用户推荐的item数量
     * @param rank          分解后小矩阵维度，即user/item隐藏向量维度
     */
    def ALS(sparkSession: SparkSession,
            orgDf: DataFrame,
            userFeature: String,
            itemFeature: String,
            ratingFeature: String,
            recommendNum: Int = 100,
            rank: Int = 100): DataFrame = {

        import sparkSession.implicits._
        val userNum = orgDf.select(userFeature).distinct().count().toInt
        val itemNum = orgDf.select(itemFeature).distinct().count().toInt
        val userItemScoreDf = orgDf
            .withColumn("user_hash", hashStringUdf(userNum + 1)(col(userFeature)))
            .withColumn("item_hash", hashStringUdf(itemNum + 1)(col(itemFeature)))
        val model: ALSModel = new ALS()
            .setUserCol("user_hash")
            .setItemCol("item_hash")
            .setRatingCol(ratingFeature)
            .setRank(rank)
            .fit(userItemScoreDf)
        val userRecommendDf = model.recommendForAllUsers(recommendNum)
            .selectExpr("user_hash", "explode(recommendations) as temp")
            .withColumn("item_hash", col("temp")("item_hash"))
            .withColumn("rating", col("temp")("rating"))
            .join(userItemScoreDf.select("user_hash", userFeature), Seq("user_hash")).drop("user_hash")
            .join(userItemScoreDf.select("item_hash", itemFeature), Seq("item_hash")).drop("item_hash")
            .groupBy(userFeature).agg(collect_list(struct(itemFeature, "rating")).as("struct"))
            .rdd
            .map(r => {
                val user = r.getAs[String](userFeature)
                val map: Map[String, Float] = r.getAs[collection.mutable.WrappedArray[GenericRowWithSchema]]("struct")
                    .map(row => (row.getString(0), row.getFloat(1)))
                    .toMap
                (user, map)
            })
            .toDF(userFeature, "similarItemByALS")
        userRecommendDf
    }
}
