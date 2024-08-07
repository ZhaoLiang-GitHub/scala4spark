package com.xm.sdalg.commons

import com.xm.sdalg.commons.ClusterUtils._
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.collection.{immutable, mutable}

object HBaseUtils extends Serializable {
    def tableName(locale: String, biz: String = "video") = {
        Map(
            singapore -> Map(
                "video" -> "hbase://alsgsrv-xm-ssd/content_alg_video:video_recommend",
                "game" -> "hbase://alsgcloudsrv-xm-ssd/content_alg_game:game_recommend"),
            holland -> Map(
                "video" -> "hbase://azamssrv-xm-ssd/content_alg_video:video_recommend",
                "game" -> "hbase://azamssrv-xm-ssd/content_alg_game:game_recommend")
        ).collectFirst({
            case (cluster, map) if localeInCluster(locale, cluster) => map(biz)
        }).getOrElse("")
    }

    def cluster(locale: String, biz: String = "video") = {
        Map(
            singapore -> Map(
                "video" -> "alsgsrv-xm-ssd",
                "game" -> "alsgcloudsrv-xm-ssd"),
            holland -> Map(
                "video" -> "azamssrv-xm-ssd",
                "game" -> "azamssrv-xm-ssd")
        ).collectFirst({
            case (cluster, map) if localeInCluster(locale, cluster) => map(biz)
        }).getOrElse("")

    }

    def fileLoadPath(locale: String, biz: String = "video") = {
        Map(
            singapore -> Map(
                "video" -> "hdfs://alsgsrv-xm-ssd/user/h_sns/bulkload/video_item_feature/",
                "game" -> "hdfs://alsgcloudsrv-xm-ssd/user/s_workspace_13936_krb/bulkload/game_center_item_feature/"),
            holland -> Map(
                "video" -> "hdfs://azamssrv-xm-ssd/user/s_workspace_14053_krb/bulkload/video_item_feature/",
                "game" -> "hdfs://azamssrv-xm-ssd/user/s_workspace_13936_krb/bulkload/game_center_item_feature/")
        ).collectFirst {
            case (cluster, map) if localeInCluster(locale, cluster) => map(biz)
        }.getOrElse("")
    }

    val FAMILY = "D"


    /**
     * BulkLoad数据到Hbase.
     *
     * @param rddForHbase  KVRdd.
     * @param sc           SparkContext.
     * @param fileName     load的文件名.
     * @param fileLoadPath BulkLoad过程中需要写入的文件路径.
     * @param tablePath    Hbase表路径.
     * @param cluster      Hbase集群.
     * @return
     */
    def doBulkLoadToHbase(rddForHbase: RDD[(ImmutableBytesWritable, KeyValue)],
                          sc: SparkContext,
                          fileName: String,
                          fileLoadPath: String,
                          tablePath: String,
                          cluster: String): Unit = {
        println(s"参数列表是 ${fileName}, ${fileLoadPath}, ${tablePath}, ${cluster}")
        val filePath = String.format("%s%s", fileLoadPath, fileName)
        HDFSUtils.delete(sc, filePath)
        //将日志保存到指定目录

        val configuration = sc.hadoopConfiguration
        val hTable = new HTable(configuration, tablePath)

        rddForHbase.saveAsNewAPIHadoopFile(filePath,
            classOf[ImmutableBytesWritable],
            classOf[KeyValue],
            classOf[HFileOutputFormat2],
            configuration)

        HDFSUtils.chmod777(filePath, configuration)

        configuration.set("fs.defaultFS", "hdfs://" + cluster)
        System.setProperty("hadoop.cmdline.hbase.cluster", "hbase://" + cluster)
        configuration.set("hbase.hregion.max.filesize", "10737418240")
        configuration.set("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", "4096")
        val load = new LoadIncrementalHFiles(configuration)
        load.doBulkLoad(new Path(filePath), hTable)
        hTable.close()
        HDFSUtils.delete(sc, filePath)
        println("HBase数据导入完成")
    }

    def renameColumn(df: DataFrame,
                     featurePre: String,
                     featureLast: String,
                     filterCols: Set[String],
                     idFieldName: String): DataFrame = {
        var dfRenamed = df
        if (StringUtils.isNotBlank(featurePre) || StringUtils.isNotBlank(featureLast)) {
            val reNameColArray = df.columns.filter(x => (!filterCols.contains(x)) && (!idFieldName.equals(x))).distinct
            reNameColArray.foreach(col => dfRenamed = dfRenamed.withColumnRenamed(col, featurePre + col + featureLast))
        }
        dfRenamed
    }

    def getFeatureColumns(df: DataFrame, filterCols: Set[String]): Array[String] = {
        df.columns.filter(x => !filterCols.contains(x)).distinct
    }

    def getValueAny(valueOrg: Any): Any = {
        if (null == valueOrg) return null
        val valueAny: Any = valueOrg match {
            case d: Double => Array(d.toFloat)
            case f: Float => Array(f)
            case i: Int => Array(i.toFloat)
            case l: Long => Array(l.toFloat)
            case str: String => str
            case arr: mutable.WrappedArray[Any] => this.matchArrayValue(arr)
            case m: immutable.Map[Any, Any] => this.matchMap(m)
            case _: Any => null
        }
        valueAny
    }

    def matchArrayValue(arr: mutable.WrappedArray[Any]): Any = {
        if (arr.nonEmpty) {
            var fArray: Array[Float] = Array()
            var strArray: Array[String] = Array()
            arr(0) match {
                case _: Double => arr.foreach(e => fArray :+= e.asInstanceOf[Double].toFloat); fArray
                case _: Float => arr.foreach(e => fArray :+= e.asInstanceOf[Float]); fArray
                case _: Int => arr.foreach(e => fArray :+= e.asInstanceOf[Int].toFloat); fArray
                case _: String => arr.foreach(e => strArray :+= e.asInstanceOf[String]); strArray
                case _ => null
            }
        } else {
            null
        }
    }

    def matchMap(m: immutable.Map[Any, Any]): Any = {
        val javaMap = new java.util.HashMap[String, Any]()
        m.foreach({ case (k, v) => {
            val newV = v match {
                case d: Double => d.toFloat
                case f: Float => f
                case i: Int => Array(i.toFloat)
                case str: String => str
                case arr: mutable.WrappedArray[Any] => this.matchArrayValue(arr)
                case valueM: immutable.Map[Any, Any] => matchMap(valueM)
                case _: Any => null
            }
            if (newV != null) javaMap.put(k.toString, newV)
        }
        })
        if (javaMap.isEmpty) null else javaMap
    }

    def checkOnlyOneZeroFloatArray(fArr: Array[Float]): Boolean = {
        fArr.length == 1 && fArr(0) == 0.0f
    }

    import org.apache.commons.codec.digest.DigestUtils

    def generateId(country: String, docType: String, id: String): String = {
        val key = country + "." + docType + "." + id
        val sha: Array[Byte] = DigestUtils.sha1(key)
        val hash: Byte = sha(0)
        val sHash: String = Integer.toHexString(hash & 0xFF)
        val sBuilder = new mutable.StringBuilder
        if (sHash.length == 1) sBuilder.append("0")
        val ret = sBuilder.append(sHash).append(":").append(key).toString
        ret
    }

    def generateId(appName: String, country: String, docType: String, id: String): String = {
        val key = appName + "." + country + "." + docType + "." + id
        val sha: Array[Byte] = DigestUtils.sha1(key)
        val hash: Byte = sha(0)
        val sHash: String = Integer.toHexString(hash & 0xFF)
        val sBuilder = new mutable.StringBuilder
        if (sHash.length == 1) sBuilder.append("0")
        val ret = sBuilder.append(sHash).append(":").append(key).toString
        ret
    }

    def generateItemId(appId: String, country: String, docType: String, id: String): String = {
        val key = appId + "." + country + "." + docType + "." + id
        val sha: Array[Byte] = DigestUtils.sha1(key)
        val hash: Byte = sha(0)
        val sHash: String = Integer.toHexString(hash & 0xFF)
        val sBuilder = new mutable.StringBuilder
        if (sHash.length == 1) sBuilder.append("0")
        val ret = sBuilder.append(sHash).append(":").append(key).toString
        ret
    }

    def generateUserId(appId: String, country: String, docType: String, docId: String): String = {
        val key =
            if (StringUtils.isBlank(country))
                appId + "." + docType + "." + docId
            else
                appId + "." + country + "." + docType + "." + docId
        val sha: Array[Byte] = DigestUtils.sha1(key)
        val hash: Byte = sha(0)
        val sHash: String = Integer.toHexString(hash & 0xFF)
        val sBuilder = new mutable.StringBuilder
        if (sHash.length == 1) sBuilder.append("0")
        val ret = sBuilder.append(sHash).append(":").append(key).toString
        ret
    }

    def showKvRddRowKey(rdd: RDD[(ImmutableBytesWritable, KeyValue)], topN: Int = 100) = {
        rdd
            .map(row => {
                val rowKeyBytes: Array[Byte] = row._1.get()
                val rowKeyStr: String = Bytes.toString(rowKeyBytes)
                (rowKeyStr, row._2.getValue)
            })
            .take(topN).foreach(println)
    }
}
