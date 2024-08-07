package com.xm.sdalg.commons

import com.google.gson._
import com.xm.data.appstore.sdalg.recommend.GetAppsRecommendSrvNewLog
import com.xm.data.commons.spark.HdfsIO.SparkSessionThriftFileWrapper
import com.xm.sdalg.appstrore_recommend.Utils.appstore_mipicks_recommend_srv_log
import com.xm.sdalg.commons.TimeUtils._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FsShell, Path}
import org.apache.hadoop.util.ToolRunner
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.thrift.TBase
import com.xm.sdalg.commons.ClusterUtils._
import com.xm.data.commons.spark.HdfsIO.SparkContextThriftFileWrapper

import java.io.PrintWriter
import scala.reflect.ClassTag

object HDFSUtils extends Serializable {

    /**
     * 删除hdfs地址
     * */
    def delete(sc: SparkContext, path: String): Boolean = {
        val fileSystem = getFileSystem(sc, path)
        fileSystem.delete(new Path(path), true)
    }

    /**
     * get file system
     */
    def getFileSystem(sc: SparkContext, path: String): FileSystem = {
        new Path(path).getFileSystem(sc.hadoopConfiguration)
    }

    /**
     * change the mode status of path
     */
    def chmod777(path: String, configuration: Configuration): Int = {
        val fsShell: FsShell = new FsShell(configuration)
        ToolRunner.run(fsShell.getConf, fsShell, Array("-chmod", "-R", "777", path))
    }

    /**
     * 判断给定路径是否存在
     */
    def exists(sc: SparkContext, path: String): Boolean = {
        val fileSystem = getFileSystem(sc, path)
        fileSystem.exists(new Path(path))
    }

    /**
     * 明文数据写入指定名字的hdfs地址中
     */
    def arraySaveInAssignPath(sc: SparkContext, assignPath: String, dataArray: Array[String]): Unit = {
        if (exists(sc, assignPath)) delete(sc, assignPath)
        val fs = FileSystem.get(sc.hadoopConfiguration)
        val writer = new PrintWriter(fs.create(new Path(assignPath)))
        dataArray.foreach(x => {
            writer.println(x)
        })
        writer.close()
    }

    /**
     * df 写入指定文件名称中
     *
     * @param assignPath 指定的文件名称
     * @param fileType   明文文件类型，TXT、csv
     */
    def dfSaveInAssignPath(sparkSession: SparkSession,
                           assignPath: String,
                           df: DataFrame,
                           fileType: String = "csv",
                           options: Map[String, String] = Map("header" -> "true")): Unit = {
        if (exists(sparkSession.sparkContext, assignPath)) delete(sparkSession.sparkContext, assignPath)
        val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
        val tempPathArray = assignPath.split("/")
        val tempPath = tempPathArray.slice(0, tempPathArray.length - 1).mkString("/") // 路径
        val fileName = tempPathArray(tempPathArray.length - 1) // 文件名
        fileType match {
            case "csv" => df.repartition(1).write.mode("overwrite").options(options).csv(tempPath)
            case "txt" => df.repartition(1).write.mode("overwrite").text(tempPath)
        }
        fs.listStatus(new Path(tempPath)).foreach(ii => {
            if (!ii.getPath.toString.contains("_SUCCESS")) {
                val tempFilePathArray = ii.getPath.toString.split("/")
                fs.rename(
                    new Path(ii.getPath.toString),
                    new Path(tempFilePathArray.slice(0, tempFilePathArray.length - 1).mkString("/") + s"/${fileName}")
                )
            }
        })
        println("文件名修改完成")
    }

    /**
     * 将dataframe写入数据工厂alpha中的hive表
     * 默认将该dataframe的数据按照schema顺序写入
     * 如果有特殊顺序，修改col列
     *
     * @param catalog   alpha的空间
     * @param partition 分区方式，默认以时间为分区
     */
    def dfSaveInAlpha(sparkSession: SparkSession,
                      catalog: String, database: String, table: String,
                      orgDf: DataFrame,
                      partition: Map[String, String]): Unit = {
        orgDf.createOrReplaceTempView("temp")
        sparkSession.sql(
            s"""
               |insert overwrite $catalog.$database.$table partition(${partition.map(r => s"${r._1} = '${r._2}'").toArray.mkString(",")})
               |select * from temp
               |""".stripMargin)
    }

    /**
     * 三级分区的数据增加分区
     *
     * @param partition 分区方式，key是分区的键，value是值
     *                  声明分区后，会自动将 hdfs路径下的数据与分区数据联系在一起
     *                  当分区数据ttl过期后，hdfs数据也会被删除
     *                  通过 desc extended appstore.dim_appstore_recommend_crowdpack partition(date=20221201,tag='game'); 查看分区的location
     */
    def alterTableIPartitionInAlpha(sparkSession: SparkSession,
                                    catalog: String, database: String, table: String,
                                    partition: Map[String, String]
                                   ): Unit = {
        sparkSession.sql(
            s"""
               |alter table $catalog.$database.$table add if not exists partition(${partition.map(r => s"${r._1} = '${r._2}'").toArray.mkString(",")})
               |""".stripMargin)
    }

    /**
     * 将RDD[String]保存在hdfs上
     *
     * @param rdd     生成的RDD，每一行是一个JSON字符串
     * @param docPath 要保存的HDFS路径
     */
    def rddSaveInHDFS(sparkSession: SparkSession, rdd: RDD[String], docPath: String, numPartitions: Int): Unit = {
        rddSaveInHDFS(sparkSession, rdd.repartition(numPartitions), docPath)
    }

    def rddSaveInHDFS(sparkSession: SparkSession, rdd: RDD[String], docPath: String): Unit = {
        val fs: FileSystem = new Path(docPath).getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
        fs.delete(new Path(docPath), true)
        rdd.saveAsTextFile(docPath)
    }


    /**
     * 读取SequenceFile到RDD
     *
     * @param spark
     * @param path
     * @param thriftClass
     * @tparam T 泛型类型
     * @return
     */
    def getSeqRdd[T <: TBase[_, _] : ClassTag](spark: SparkSession, path: String, thriftClass: Class[T]): RDD[T] = {
        spark.thriftSequenceFile(path, thriftClass)
    }

    /**
     * 在给定的日期范围内找到给定路径中包含_SUCCESS的最新日期的路径
     * 给定路径中需要包含 /date=yyyyMMdd 查找时替换该字符串为 /date=20220801 进行查找
     */
    def getLatestExistPath(sparkSession: SparkSession, orgPath: String, lastDate: String, dateDiff: Int = 30): String = {
        val resultDate: String = getLatestExistDate(sparkSession, orgPath, lastDate, dateDiff)
        if (resultDate != null)
            orgPath.replace("date=yyyyMMdd", s"date=$resultDate")
        else {
            println(s"在给定时间范围内没有找到${orgPath}的_SUCCESS文件")
            null
        }
    }

    /**
     * 在给定的时间diff内查找还有最新_SUCCESS文件路径的日期
     */
    def getLatestExistDate(sparkSession: SparkSession, orgPath: String, lastDate: String, dateDiff: Int = 30): String = {
        var resultDate: String = null
        for (d <- 0 to dateDiff if resultDate == null) {
            val successPath = orgPath.replace("date=yyyyMMdd", s"date=${getFrontDay(lastDate, d)}") + "/_SUCCESS"
            if (exists(sparkSession.sparkContext, successPath)) {
                println(s"当前给定的路径${orgPath}中最新日期是 ${getFrontDay(lastDate, d)}")
                resultDate = getFrontDay(lastDate, d)
            }
        }
        if (resultDate == null)
            println(s"在给定时间范围内没有找到${orgPath}的_SUCCESS文件")
        resultDate
    }

    /**
     * 在给定的时间范围内，读取parquet文件
     */
    def readParquetBetweenStartdateAndEnddate(sparkSession: SparkSession, hivePath: String, startDate: String, endDate: String): DataFrame = {
        val dateDiff = getYmdGapDays(endDate, startDate)
        readParquetFromEnddateAndDatediff(sparkSession, hivePath, dateDiff + 1, endDate)
    }

    /**
     * 给定结束时间和时间范围，读取parquet文件
     */
    def readParquetFromEnddateAndDatediff(sparkSession: SparkSession, hivePath: String, dateDiff: Int, endDate: String): DataFrame = {
        var result: DataFrame = null
        Range(0, dateDiff).foreach(d => {
            val date: String = getFrontDay(endDate, d)
            val temp = s"${hivePath}/date=${date}"
            if (exists(sparkSession.sparkContext, temp + "/_SUCCESS")) {
                val tempDf = sparkSession.read.parquet(temp)
                    .withColumn("date", lit(date))
                if (result == null) result = tempDf
                else result = result.unionByName(tempDf)
            }
        })
        result
    }


    /**
     * 在alpha上的spark3.1 中通过spark.read.table 的方式读取数据
     * 时间范围是 [startDate,endDate]
     * 注：在离线机器的spark2.1 2.3中使用会报错
     */
    def readTableInAlphaBetweenDate(sparkSession: SparkSession,
                                    catalog: String, database: String, table: String,
                                    startDate: String, endDate: String): DataFrame = {
        readTableInAlphaBetweenDate(sparkSession, s"$catalog.$database.$table", startDate, endDate)
    }

    def readTableInAlphaBetweenDate(sparkSession: SparkSession, table: String, startDate: String, endDate: String): DataFrame = {
        sparkSession.table(s"$table").where(s"date between '$startDate' and '$endDate'")
    }

    /**
     * 在alpha上读取非时间分区表
     */
    def readTableInAlpha(sparkSession: SparkSession,
                         catalog: String, database: String, table: String): DataFrame = {
        readTableInAlpha(sparkSession, s"$catalog.$database.$table")
    }

    def readTableInAlpha(sparkSession: SparkSession, table: String): DataFrame = {
        sparkSession.read.table(table)
    }

    /**
     * 将明文json数据读成dataframe
     */
    def readJsonInHdfs(sparkSession: SparkSession, path: String): DataFrame = {
        val temp = sparkSession.sqlContext.read.json(sparkSession.read.text(path).rdd.map(row => row.getString(0)))
        temp
    }

    /**
     * 从hdfs中获得JSON对象
     *
     * @param path hdfs地址，第一行是一个JSON字符串
     */
    def text2json(sparkSession: SparkSession, path: String): JsonObject = {
        new JsonParser().parse(sparkSession.sparkContext.textFile(path).collect()(0)).getAsJsonObject
    }


    def union(result: RDD[String], temp: RDD[String]): RDD[String] = {
        if (result == null) temp
        else result.union(temp)
    }

    def union(result: DataFrame, temp: DataFrame): DataFrame = {
        if (result == null) temp
        else result.unionByName(temp)
    }

    /**
     * 获得iceberg表中最新日期
     */
    def getLastIcebergDate(sparkSession: SparkSession, iceberg: String, endDate: String = null): String = {
        val date = if (endDate == null) getCurrentDateStr() else endDate
        sparkSession.sql(s"select partition from ${iceberg}.partitions")
            .select(col("partition").getField("date").cast("int"))
            .rdd
            .map(_.getInt(0))
            .collect()
            .filter(_ <= date.toInt)
            .max
            .toString
    }

    /**
     * 读取iceberg表中最新日期的数据
     */
    def readLastDateIceberg(sparkSession: SparkSession, iceberg: String, endDate: String = null): DataFrame = {
        val date = if (endDate == null) getCurrentDateStr() else endDate
        val lasDate = getLastIcebergDate(sparkSession, iceberg, date)
        sparkSession.sql(s"select * from ${iceberg} where date = '${lasDate}'")
    }

    /**
     * 获得hive表中最新日期
     */
    def getLastHiveDate(sparkSession: SparkSession, hive: String, endDate: String = null): String = {
        val date = if (endDate == null) getCurrentDateStr() else endDate
        sparkSession.sql(
            s"""
               |select cast(max(date) as string)
               |from ${hive}
               |where date between '${getFrontDay(date, 30)}' and '${date}'
               |""".stripMargin)
            .rdd
            .map(_.getString(0))
            .first()
    }

    /**
     * 读取hive表中最新日期的数据
     */
    def readLastDateHive(sparkSession: SparkSession, hive: String, endDate: String = null): DataFrame = {
        val date = if (endDate == null) getCurrentDateStr() else endDate
        val lastDate = getLastHiveDate(sparkSession, hive, date)
        sparkSession.sql(s"select * from ${hive} where date = '${lastDate}'")
    }
}
