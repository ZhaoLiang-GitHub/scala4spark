package com.xm.sdalg.appstrore_recommend

import com.xm.sdalg.commons.SparkContextUtils.createSparkSession
import org.apache.spark.sql.SparkSession


object Test {

    def main(args: Array[String]): Unit = {
        val Array(locale: String, startDate: String, endDate: String, frequencyGame: String, durationGame: String) = args
        val sparkSession: SparkSession = createSparkSession(this.getClass.getSimpleName)
    }
}
