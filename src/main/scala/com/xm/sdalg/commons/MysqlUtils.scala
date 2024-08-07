package com.xm.sdalg.commons

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import java.sql.DriverManager

object MysqlUtils extends Serializable {
    // 实验平台的MySQL配置
    val c4_mig3_sdalg_stage00 = "" // c400 机器的IP地址
    val mysqlPort = "" // mysql 默认的端口号
    val abtestDataBase = "" // abtest 数据库名
    val abtestUser = "" // abtest 数据库的用户名
    val abtestPassword = "" // abtest 数据库的密码
    val abtestAbdataTable = "" // abtest 数据库的表名，用于存储实验原始数据，主要是四列，data, tag, index_key, index_value, insert_time
    val abtestColumn: Array[String] = Array("date", "tag", "index_key", "index_value", "insert_time") // new_abdata 表的列名


    // 热搜榜写入的物料的MySQL配置
    val hotSearchUser = ""
    val hotSearchPassword = ""
    val hotSearchIp = ""
    val hotSearchPort = ""
    val hotSearchDataBase = ""
    val hotSearchTable = ""

    /**
     * 从mysql中读取数据
     *
     * @param host     IP地址
     * @param port     端口号
     * @param database 数据库名
     * @param table    表名
     * @param user     用户名
     * @param password 密码
     * @example readFromMysql(sparkSession, c4_mig3_sdalg_stage00, mysqlPort, abtestDataBase, "abdata", abtestUser, abtestPassword)
     */
    def readFromMysql(sparkSession: SparkSession,
                      ip: String,
                      port: String,
                      database: String,
                      table: String,
                      user: String,
                      password: String): DataFrame = {
        sparkSession.read
            .format("jdbc")
            .options(Map(
                "url" -> s"jdbc:mysql://$ip:$port/$database",
                "dbtable" -> table,
                "user" -> user,
                "password" -> password
            ))
            .load()
    }

    /**
     * 将数据写入到mysql中
     *
     * @param rdd    需要写入的数据,每一行是一个Array[String]，是按照要写入位置与要写入如的列名一一对应
     * @param column 要写入的列名
     */
    def writeToMysql(sparkSession: SparkSession,
                     rdd: RDD[Array[String]],
                     column: Array[String],
                     ip: String,
                     port: String,
                     database: String,
                     table: String,
                     user: String,
                     password: String,
                     numPartitions: Int = 1000,
                     maxTryNum: Int = 4) = {
        val error = sparkSession.sparkContext.longAccumulator("errorLineNum")
        val success = sparkSession.sparkContext.longAccumulator("successLineNum")
        rdd2sql(rdd, column, table)
            .repartition(numPartitions)
            .foreachPartition(part => {
                val connection = DriverManager.getConnection(s"jdbc:mysql://$ip:$port/$database", user, password)
                val statement = connection.createStatement()
                part.foreach(sql => {
                    var isSuccess = false
                    Range(1, maxTryNum).foreach(_ => {
                        if (!isSuccess)
                            try {
                                statement.execute(sql)
                                isSuccess = true
                            } catch {
                                case exception: Exception =>
                                    println(sql, exception)
                            }
                    })
                    if (!isSuccess)
                        error.add(1L)
                    else
                        success.add(1L)
                })
                connection.close()
            })
        println("写入成功的数据量为：" + success.value)
        println("写入失败的数据量为：" + error.value)
        println("写入Mysql完成")
        if (error.value.toDouble / (success.value + error.value).toDouble > 0.1)
            throw new Exception("写入失败的数据量超过10%")
    }

    /**
     * 将rdd转换为sql语句
     */
    def rdd2sql(rdd: RDD[Array[String]],
                column: Array[String],
                table: String): RDD[String] = {
        println(s"原始sql共有${rdd.count()}条")
        rdd
            .filter(_.length == column.length)
            .map(row => {
                s"""
                   |insert into $table (${column.mkString(",")}) values ('${row.mkString("','")}') ON DUPLICATE KEY UPDATE ${
                    Range(0, column.length).map(i => {
                        s"${column(i)}='${row(i)}'"
                    }).mkString(",")
                };
                   |""".stripMargin
            })
    }
}