package com.xm.sdalg.commons

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * sparkContext相关.
 */
object SparkContextUtils extends Serializable {

    /**
     * 创建sparkSession
     *
     * @param appName       例如：this.getClass.getSimpleName
     * @param beLocal       是否配置为Local运行模式
     * @param shuffleNumCfg shuffle的分区数，例如："1024"等
     * @param logLevel      日志级别，Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
     * @return
     */
    def createSparkSession(appName: String,
                           beLocal: Boolean = false,
                           shuffleNumCfg: String = "",
                           logLevel: String = ""): SparkSession = {
        var builder: SparkSession.Builder = SparkSession
            .builder
            .appName(appName)
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        if (beLocal) builder = builder.master("local[*]")
        val sparkSession: SparkSession = if (beLocal) builder.getOrCreate() else builder.enableHiveSupport().getOrCreate()
        if (StringUtils.isNotBlank(shuffleNumCfg)) sparkSession.sqlContext.setConf("spark.sql.shuffle.partitions", shuffleNumCfg)
        if (StringUtils.isNotBlank(logLevel)) sparkSession.sparkContext.setLogLevel(logLevel)
        sparkSession.sqlContext.setConf("spark.sql.optimizer.insertRepartitionBeforeWrite.enabled", "true")
        sparkSession.sqlContext.setConf("spark.sql.optimizer.finalStageConfigIsolation.enabled", "true")
        sparkSession
    }

    def JoinDf(res: Option[DataFrame], other: DataFrame, joinColumns: Seq[String], joinType: String = "inner") = {
        res match {
            case Some(nonEmptyDF) =>
                nonEmptyDF.join(other, joinColumns, joinType)
            case None =>
                other
        }
    }
}
