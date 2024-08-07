package com.xm.sdalg.commons

import com.xm.sdalg.commons.SampleUtils.Alias
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}

import java.io.Serializable
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, PriorityQueue}
import scala.util.Random

/**
 * @author zhaoliang6 on 20221009
 *         基于随机游走的图模型的相关算法的抽象实现
 */
object RandomWalkUtils extends Serializable {
    /**
     * 图中每个节点的属性类
     *
     * @param neighbors 每个节点的邻接节点与对应边权重
     * @param alias     基于邻接节点与边权重生成的采样算法对象
     */
    case class NodeAttr(var id: Long = -1L,
                        var neighbors: Array[(Long, Double)] = Array.empty[(Long, Double)],
                        var alias: Alias = null) extends Serializable


    /**
     * 图中每条边的属性类
     *
     */
    case class EdgeAttr() extends Serializable


    /**
     * 基于spark自带的word2vec模型将item序列转化成向量
     * 所有的参数与 getWord2VecModel 函数含义相同
     */
    def getItemVecByW2V(orgDf: DataFrame,
                        inputCol: String,
                        outputCol: String = "vector",
                        maxIter: Int = 2,
                        minCount: Int = 500,
                        vecLength: Int = 128,
                        windowSize: Int = 5,
                        maxSeqLength: Int = 20,
                        numPartition: Int = 10000): DataFrame = {
        val vector2Array: UserDefinedFunction = udf((vec: Vector) => vec.toArray)
        val model = getWord2VecModel(orgDf, inputCol, "vectorTemp", maxIter, minCount, vecLength, windowSize, maxSeqLength, numPartition)
        val itemVec: DataFrame = model.getVectors
            .withColumnRenamed("vector", "vectorTemp")
            .withColumn(outputCol, vector2Array(col("vectorTemp")))
            .selectExpr("word as item", outputCol)
        itemVec
    }

    /**
     * 获得spark自带的word2vector模型
     *
     * @param orgDf        输入的dataframe，需要包含inputCol列,inputCol列必须是 Seq[String]类型
     * @param maxIter      最大迭代轮数
     * @param minCount     每个单词（item）出现的最小次数
     * @param vecLength    生成的向量长度
     * @param windowSize   在skipGram中窗口的大小
     * @param maxSeqLength 句子序列的最长长度
     * @param numPartition 分区数
     * @return
     */
    def getWord2VecModel(orgDf: DataFrame,
                         inputCol: String,
                         outputCol: String,
                         maxIter: Int = 2,
                         minCount: Int = 500,
                         vecLength: Int = 128,
                         windowSize: Int = 5,
                         maxSeqLength: Int = 20,
                         numPartition: Int = 10000): Word2VecModel = {
        val model = new Word2Vec()
            .setInputCol(inputCol)
            .setOutputCol(outputCol)
            .setMaxIter(maxIter)
            .setMinCount(minCount)
            .setVectorSize(vecLength)
            .setWindowSize(windowSize)
            .setNumPartitions(numPartition)
            .setMaxSentenceLength(maxSeqLength)
            .fit(orgDf)
        println(model)
        model
    }


    /**
     * 将采样之后的long序列结果转回原始的string对象
     *
     * @param orgRdd         从输入orgDf通过getString2Long函数转成的对象
     * @param pathRdd        采样之后的结果，每一行是采样之后的原始路径
     * @param node2indexDf   long与string互转的对象
     * @param selectNodeType 需要采样的节点类型，在原始的orgDf中输入的头节点、尾节点、全部节点
     * @return 转换之后的string类型的采样结果的路径
     */
    def getLong2String(sparkSession: SparkSession,
                       orgRdd: RDD[(Long, Long, Double)],
                       pathRdd: RDD[Array[Long]],
                       node2indexDf: DataFrame,
                       selectNodeType: String = "end"): DataFrame = {
        import sparkSession.implicits._
        val allNode = selectNodeType match {
            case "start" => orgRdd.map(_._1).distinct().toDF("index")
            case "end" => orgRdd.map(_._2).distinct().toDF("index")
            case "all" => orgRdd.flatMap(row => Array(row._1, row._2)).distinct().toDF("index")
        }
        val itemSetMapBC: Broadcast[collection.Map[VertexId, String]] = sparkSession.sparkContext.broadcast(
            node2indexDf
                .join(allNode, Seq("index"))
                .selectExpr("index", "node")
                .rdd
                .map(row => (row.getLong(0), row.getString(1)))
                .collectAsMap()
        )
        println(s"当前序列采样结果中共有item数量是 ${itemSetMapBC.value.size}")
        val result: DataFrame = pathRdd
            .map({ path: Array[Long] =>
                path
                    .map(ii => itemSetMapBC.value.getOrElse(ii, "-1"))
                    .filter(_ != "-1")
            })
            .filter(_.length > 0)
            .toDF("path")
        result.show(false)
        result.printSchema()
        result
    }

    /**
     * 对输入的原始的dataframe做转化，将startNode、endNode节点的string对象转成在graph中的Long对象
     *
     * @return 转换后的rdd对象，以及string2long互转的对象
     */
    def getString2Long(sparkSession: SparkSession, df: DataFrame): (RDD[(VertexId, VertexId, Double)], DataFrame) = {
        import sparkSession.implicits._
        val column = df.columns
        val orgDf = df
            .selectExpr(s"cast(${column(0)} as string) as startNode",
                s"cast(${column(1)} as string) as endNode",
                s"cast(${column(2)} as double) as weight")
            .select("startNode", "endNode", "weight")
            .cache()
        println(s"原始数据中的边的数量是 ${orgDf.count()}")
        val node2indexDf = orgDf // string的id与long型的index的映射
            .rdd
            .flatMap(row => Array(row.getString(0), row.getString(1)))
            .distinct()
            .zipWithIndex()
            .toDF("node", "index")
        println(s"原始数据中的节点数量是 ${node2indexDf.count()}")

        val orgRdd: RDD[(Long, Long, Double)] = orgDf
            .join(node2indexDf.selectExpr("node as startNode", "index as index1"), Seq("startNode"))
            .join(node2indexDf.selectExpr("node as endNode", "index as index2"), Seq("endNode"))
            .selectExpr("index1", "index2", "weight")
            .rdd
            .map(row => (row.getLong(0), row.getLong(1), row.getDouble(2)))
            .repartition(10000)
        (orgRdd, node2indexDf)
    }


    /**
     * 基于DFS的随机游走实现从图结构中进行采样构造序列进行向量化
     * 将输入的startNode-endNode-weight 对所有的节点进行group，获得每个节点的邻接矩阵及权重
     * 通过alias(别名采样法)获得每个节点的采样方式
     * 对于每个节点出发构造序列，以当前节点的采样方式来获得下一个节点，将下一个节点加入序列中，以下一个节点为当前序列的尾结点
     *
     * @param orgDf     输入的原始数据，共3列，startNode,endNode,weight
     *                  weight表示当前一对儿关系的权重，即带权重的双向图，如果没有则全部为1
     *                  在实际的user-item-score 二部图中，startNode全部为userid，endNode全部为itemid
     *                  在获得游走序列结果之后需要使用spark自带的word2vector模型进行训练词向量
     *                  最终需要在序列中仅过滤出itemid，否则会将出现过的所有的id都进行训练
     * @param seqLength 游走的序列长度
     *                  如果是UI二部图的话，则直接DFS游走的结果是user-item-user-item
     *                  所以该长度应该是实际想要的序列长度的2倍
     * @param bfsRatio  如果当walkType="dfs&bfs"时，在每个节点的bfs概率
     */
    def DeepWalk(sparkSession: SparkSession,
                 orgDf: DataFrame,
                 seqLength: Int = 20,
                 selectNodeType: String = "end",
                 isDirectedGraph: Boolean = false,
                 walkType: String = "dfs",
                 bfsRatio: Double = 0.5): DataFrame = {
        val (orgRdd, node2indexDf) = getString2Long(sparkSession, orgDf)

        val nodeRdd: RDD[(VertexId, Alias)] = orgRdd
            .flatMap({ case (startNode, endNode, weight) =>
                if (isDirectedGraph)
                    Array((startNode, (endNode, weight)))
                else
                    Array((startNode, (endNode, weight)), (endNode, (startNode, weight)))
            })
            .groupByKey()
            .map(row => {
                (row._1, new Alias(row._2.toArray).setAlias)
            })
            .cache()

        var randomWalkPaths: RDD[(VertexId, ArrayBuffer[VertexId])] = nodeRdd
            .map({ case (node, alias) =>
                val path = new ArrayBuffer[Long]()
                path.append(node)
                (node, path)
            })
        walkType match {
            case "dfs" =>
                for (_ <- 0 to seqLength) {
                    // 每一轮都以当前序列的尾结点进行采样，与上一个结点无关，每一步都是DFS
                    randomWalkPaths = randomWalkPaths
                        .join(nodeRdd)
                        .map({ case (vertexId, (path, alias)) =>
                            val nextNode = alias.sample
                            path.append(nextNode)
                            (nextNode, path)
                        })
                }
            case "dfs&bfs" =>
                for (_ <- 0 to seqLength) {
                    randomWalkPaths = randomWalkPaths
                        .join(nodeRdd)
                        .map({ case (vertexId, (path, alias)) =>
                            if (Random.nextDouble < bfsRatio) { //DFS
                                path.append(alias.sample)
                            } else { //BFS
                                path.append(alias.sample)
                                path.append(alias.sample)
                            }
                            (path(path.length - 1), path)
                        })
                        .repartition(10000)
                }
        }
        val pathsResult = getLong2String(sparkSession, orgRdd, randomWalkPaths.map(_._2.toArray), node2indexDf, selectNodeType)
        pathsResult
    }

    /**
     * 基于Node2Vec模型的随机游走方法
     * 与DeepWalk相比较多了两个游走控制参数q控制向内还是向外，p控制DFS、BFS
     * 其他与DeepWalk相同
     *
     * @param seqLength       采样序列长度
     * @param q               q > 1 时，q越大采样不相邻节点的概率越小，则采样其他节点（倒数第二个节点，倒数第一个节点的兄弟节点）的概率越大，即BFS
     * @param p               p > 1 时，p越大倒数第一个节点采样倒数第二个节点的概率越小，即路径往外走
     * @param maxNeighborNum  每个节点最多出现的邻接节点个数
     * @param selectNodeType  最终选择的节点类型，start(头结点) end(尾结点) all(全部节点)
     * @param isDirectedGraph 是否是有向图，如果是则保持输入的头尾关系，如果不是则头尾、尾头均能构成边
     */
    def Node2Vec(sparkSession: SparkSession,
                 orgDf: DataFrame,
                 seqLength: Int = 5,
                 selectNodeType: String = "end",
                 isDirectedGraph: Boolean = true,
                 maxNeighborNum: Int = 10000,
                 p: Double = 1,
                 q: Double = 1): DataFrame = {
        val (orgRdd, node2indexDf) = getString2Long(sparkSession, orgDf)

        val nodeAttr: RDD[(VertexId, NodeAttr)] = orgRdd
            .flatMap({ case (startNode, endNode, weight) =>
                if (isDirectedGraph)
                    Array((startNode, (endNode, weight)))
                else
                    Array((startNode, (endNode, weight)), (endNode, (endNode, weight)))
            })
            .groupByKey()
            .map({ case (nodeId: Long, iterable: Iterable[(Long, Double)]) =>
                val seq = iterable.toArray.filter(_._1 != -1L)
                if (seq.length > 0) {
                    val selectPath: Array[(VertexId, Double)] =
                        if (seq.length > maxNeighborNum) {
                            val alias = new Alias(seq).setAlias
                            val result = new ArrayBuffer[(VertexId, Double)]()
                            for (_ <- 0 until maxNeighborNum) {
                                result.append(seq(alias.sampleIndexOfIter))
                            }
                            result.toArray
                        } else
                            seq
                    (nodeId, NodeAttr(id = nodeId, neighbors = selectPath))
                }
                else {
                    (nodeId, NodeAttr(id = nodeId))
                }
            })
            .cache()
        println(s"节点数量是 ${nodeAttr.count()}")

        val edgeAttr: RDD[Edge[EdgeAttr]] = nodeAttr
            .flatMap({ case (startNode: Long, nodeAttr: NodeAttr) =>
                nodeAttr.neighbors.map({ case (endNode: Long, _: Double) =>
                    Edge(startNode, endNode, EdgeAttr())
                })
            })

        val edge2attr: RDD[(String, Array[(VertexId, Double)])] = Graph(nodeAttr, edgeAttr)
            .triplets
            .repartition(10000)
            .filter({ edgeTriplet: EdgeTriplet[NodeAttr, EdgeAttr] =>
                edgeTriplet.srcId != -1L && edgeTriplet.dstId != -1L &&
                    edgeTriplet.srcAttr != null && edgeTriplet.srcAttr.id != -1L && edgeTriplet.srcAttr.neighbors.length > 0 &&
                    edgeTriplet.dstAttr != null && edgeTriplet.dstAttr.id != -1L && edgeTriplet.dstAttr.neighbors.length > 0
            })
            .map(edgeTriplet => {
                val srcId = edgeTriplet.srcId
                val dstId = edgeTriplet.dstId
                val srcNeighborSet = edgeTriplet.srcAttr.neighbors.map(_._1).toSet
                val dstNeighbor = edgeTriplet.dstAttr.neighbors
                    .map({ case (dstNeighborId, weight) =>

                        /**
                         * BFS、DFS的关键在于在序列中的倒数第二个节点与倒数第一个节点之间的关系
                         * DFS只需要在序列的倒数第一个节点中通过随机采样/基于边权重采样
                         * 将获得的结果拼接在序列之后，作为新的倒数第一个节点继续重复采样过程即可
                         * BFS需要采样倒数第二个节点的邻接节点，即倒数第一个节点的兄弟节点
                         *
                         * 在node2vec算法中，通过两个参数来控制BFS、DFS的程度
                         * 倒数第二个节点 与 倒数第一个节点的邻接节点有三种关系
                         * 倒数第二个节点 与 邻接节点直接相连 pro = weight
                         * 倒数第二个节点 就是 邻接节点 pro = weight / p
                         * 倒数第二个节点 与 邻接节点不相连 pro = weight / q
                         * 在alias采样中，概率越大被采中的概率也越大
                         *
                         * @note p = q = 1 时，为随机采样(基于边权重)，node2vec退化为DeepWalk
                         *       0 < p < 1 时，p越小在倒数第一个节点采样倒数第二个节点的概率越大，即路径往回走
                         *       p > 1 时，p越大倒数第一个节点采样倒数第二个节点的概率越小，即路径往外走
                         *       0 < q < 1 时，q越小倒数第一个节点中采样不相邻节点的概率越大，即DFS
                         *       q > 1 时，q越大采样不相邻节点的概率越小，则采样其他节点（倒数第二个节点，倒数第一个节点的兄弟节点）的概率越大，即BFS
                         */
                        var unnormProb: Double = weight
                        if (srcId == dstNeighborId)
                            unnormProb = weight / p
                        else if (srcNeighborSet.contains(dstNeighborId))
                            unnormProb = weight
                        else
                            unnormProb = weight / q
                        (dstNeighborId, unnormProb)
                    })
                (s"$srcId,$dstId", dstNeighbor)
            })
        println(s"边的数量是 ${edge2attr.count()}")


        var randomWalkPaths: RDD[(String, ArrayBuffer[VertexId])] = nodeAttr
            .filter(_._2.neighbors.length > 0)
            .map({
                case (nodeId: Long, nodeAttr: NodeAttr) =>
                    val result = new ArrayBuffer[Long]()
                    val nextNode: VertexId = new Alias(nodeAttr.neighbors).setAlias.sample
                    result.append(nodeId)
                    result.append(nextNode)
                    (result.mkString(","), result)
            })

        for (_ <- 0 until seqLength) {
            randomWalkPaths = randomWalkPaths
                .join(edge2attr)
                .flatMap({ case (_, (path, dstNeighbor: Array[(VertexId, Double)])) =>
                    val result = new ArrayBuffer[ArrayBuffer[Long]]()
                    val alias = new Alias(dstNeighbor).setAlias
                    if (dstNeighbor.length == 0) {
                        path.append(path(path.length - 2))
                        result.append(path)
                    } else {
                        val temp = new ArrayBuffer[Long]()
                        path.copyToBuffer(temp)
                        temp.append(alias.sample)
                        result.append(temp)
                    }
                    result.map(ii => (Array(ii(ii.length - 2), ii(ii.length - 1)).mkString(","), ii)) // 最后两个节点形成新的边
                })
        }

        val pathsResult = getLong2String(sparkSession, orgRdd, randomWalkPaths.map(_._2.toArray), node2indexDf, selectNodeType)
        pathsResult.show(false)
        pathsResult.printSchema()
        pathsResult
    }

    /**
     * EGES 模型是在随机采样的基础上增加了side information信息的编码方式
     * 采样方式采用Deep、Node2Vec
     * 获得采样序列后，根据词向量模型窗口大小，获得词对儿
     * 词对儿 为Word2Vector模型中正样本，在wrod2vec模型训练时加入side information
     * 其他模型参数 deepwalk、node2vec模型含义一致
     */
    def EGES(sparkSession: SparkSession,
             orgDf: DataFrame,
             seqLength: Int = 5,
             maxNeighborNum: Int = 10000,
             p: Double = 1,
             q: Double = 1,
             selectNodeType: String = "end",
             isDirectedGraph: Boolean = true,
             randomWalkType: String = "deepwalk",
             windowSize: Int = 5): DataFrame = {
        import sparkSession.implicits._
        val pathResult = randomWalkType match {
            case "node2vec" => Node2Vec(sparkSession, orgDf, seqLength, selectNodeType, isDirectedGraph, maxNeighborNum, p, q)
            case "deepwalk" => DeepWalk(sparkSession, orgDf, seqLength, selectNodeType, isDirectedGraph)
        }
        val pairDf = pathResult
            .rdd
            .flatMap(row => {
                val seq = row.getSeq[String](0)
                val result = new ArrayBuffer[Array[String]]()
                result
            })
            .toDF("wordPair")
        pairDf
    }
}
