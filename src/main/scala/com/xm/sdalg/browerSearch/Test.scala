package com.xm.sdalg.browerSearch

import com.xm.sdalg.commons.SparkContextUtils._
import org.apache.spark.sql._

object Test {
    /**
     * 测试类，自行在各自的分支中使用，不要合并
     */
    def main(args: Array[String]): Unit = {
        val sparkSession: SparkSession = createSparkSession(this.getClass.getSimpleName)
    }
}