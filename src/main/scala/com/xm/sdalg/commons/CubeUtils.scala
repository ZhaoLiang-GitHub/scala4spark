package com.xm.sdalg.commons

import com.xm.cubedb.direct.DocumentHandler
import com.xm.miliao.zookeeper.EnvironmentType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import scala.reflect.ClassTag

object CubeUtils {

    val appstoreBehaviorTopic = "appstore_behavior"
    val documentHandler = DocumentHandler.getInstance(EnvironmentType.ALSG_CLOUDSRV)
    val expoHistoryLength = 100
    val expoHistoryLimitTime = 2 * 60 * 60 * 1000L
    val expoHistoryCatgLength = 5
    val expoHistoryCatgLimitTime = 2 * 60 * 60 * 1000L
    val frequencyLimit = 2
    val APP_ID = ""
    val DOC_TYPE = ""

    /**
     * 从离线要写入doc的hdfs路径中读取value的Map[String,Long]
     *
     * @param key         要写入doc中的数据路径
     * @param value       主键，默认是id
     * @param blackKeySet 写入数据中的主键
     * @return
     */
    def getMapFromDocPath[T: ClassTag](sparkSession: SparkSession,
                                       locale: String,
                                       path: String,
                                       dataType: String,
                                       key: String, value: String,
                                       blackKeySet: Set[String],
                                       whiteKeySet: Set[String],
                                      ): RDD[(String, Map[String, T])] = {
        def getTypedValue[T: ClassTag](row: Row, index: Int, dataType: String): Option[T] = {
            dataType match {
                case "Long" =>
                    if (row.isNullAt(index)) None else Some(row.getLong(index).asInstanceOf[T])
                case "Double" =>
                    if (row.isNullAt(index)) None else Some(row.getDouble(index).asInstanceOf[T])
                case _ => None
            }
        }

        if (HDFSUtils.exists(sparkSession.sparkContext, path)) {
            sparkSession.read.json(path)
                .selectExpr(key, value)
                .filter(row => {
                    var flag = true
                    if (blackKeySet != null) flag = !blackKeySet.contains(row.getAs[String](key))
                    flag
                })
                .filter(row => {
                    var flag = true
                    if (whiteKeySet != null) flag = whiteKeySet.contains(row.getAs[String](key))
                    flag
                })
                .rdd
                .map(row => {
                    val id = row.getString(0)
                    val temp = row.getAs[Row](1)
                    val schema = temp.schema
                    val apps: Map[String, T] = Range(0, schema.fieldNames.length)
                        .flatMap(ii => {
                            val fieldName = schema.fieldNames(ii)
                            getTypedValue[T](temp, ii, dataType).map(value => (fieldName, value))
                        })
                        .toMap
                    (id, apps)
                })
        } else {
            println(s"给定的${path}不存在")
            null
        }
    }
}
