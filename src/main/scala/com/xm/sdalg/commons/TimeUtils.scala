package com.xm.sdalg.commons

import com.xm.sdalg.commons.TimeUtils.dateDiff

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import org.apache.commons.lang3.time.{DateFormatUtils, DateUtils}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeUnit
import scala.collection.mutable.ArrayBuffer
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.DayOfWeek

object TimeUtils extends Serializable {

    val YMD_FORMAT: String = "yyyyMMdd"

    val YMD_HMS_FORMAT: String = "yyyy-MM-dd HH:mm:ss"

    val YMD_H: String = "yyyyMMddHH"

    val yyyy_MM_dd: String = "yyyy-MM-dd"

    val DAY_MILLI_SECONDS: Int = 24 * 60 * 60 * 1000

    /**
     * 获取昨天的日期字符串.
     *
     * @return
     */
    def getLastDateStr(): String = {
        val date = new Date()
        DateFormatUtils.format(DateUtils.addDays(date, -1), YMD_FORMAT)
    }

    /**
     * 获取今天的日期字符串.
     *
     * @return
     */
    def getCurrentDateStr(): String = {
        val date = new Date()
        DateFormatUtils.format(date, YMD_FORMAT)
    }

    /**
     * 获取当前时间戳.
     *
     * @return
     */
    def getCurrentTs(): Long = {
        new Date().getTime
    }


    /**
     * 获取给定日期前一天的日期字符串.
     *
     * @param yyyyMMdd
     * @return
     */
    def getPreDayStr(yyyyMMdd: String): String = this.getPreDayStr(yyyyMMdd, 1)

    /**
     * 获取给定日期前days天的日期字符串.
     *
     * @param yyyyMMdd
     * @param days
     * @return
     */
    def getPreDayStr(yyyyMMdd: String, days: Int): String = {
        val date = DateUtils.parseDate(yyyyMMdd, YMD_FORMAT)
        DateFormatUtils.format(DateUtils.addDays(date, -1 * days), YMD_FORMAT)
    }

    /**
     * 获取日期后days天的日期字符串.
     *
     * @param yyyyMMdd
     * @param days
     * @return
     */
    def getPostDayStr(yyyyMMdd: String, days: Int): String = {
        val date = DateUtils.parseDate(yyyyMMdd, YMD_FORMAT)
        DateFormatUtils.format(DateUtils.addDays(date, 1 * days), YMD_FORMAT)
    }

    /**
     * 获取明天的日期字符串.
     *
     * @param yyyyMMdd
     * @return
     */
    def getPostDayStr(yyyyMMdd: String): String = this.getPostDayStr(yyyyMMdd, 1)


    /**
     * 根据时间戳获取小时值.
     *
     * @param ts
     * @return
     */
    def getHourFromTs(ts: Long): Int = {
        val date = new Date(ts)
        val cal = Calendar.getInstance
        cal.setTime(date)
        cal.get(Calendar.HOUR_OF_DAY)
    }

    /**
     * 获得当前的小时值
     * @return
     */
    def getCurrentHour = {
        getHourFromTs(getUnixTimeStamp)
    }

    /**
     * 根据时间戳获取分钟值
     *
     * @param ts
     * @return
     */
    def getMinuteFromTs(ts: Long): Int = {
        val date = new Date(ts)
        val cal = Calendar.getInstance
        cal.setTime(date)
        cal.get(Calendar.MINUTE)
    }

    /**
     * 将24小时均分成96个桶，返回当前小时的分桶数，取值分位0-95.
     *
     * @param ts
     * @return
     */
    def getBucketNum96(ts: Long): Int = {
        val date = new Date(ts)
        val cal = Calendar.getInstance
        cal.setTime(date)
        val h = cal.get(Calendar.HOUR_OF_DAY)
        val m = cal.get(Calendar.MINUTE)
        h * 4 + m / 15
    }

    /**
     * 获取时间戳对应的字符串表示：yyyy-MM-dd HH:mm:ss
     *
     * @param ts
     * @return
     */
    def getStringTime(ts: Long): String = {
        val date = new Date(ts)
        DateFormatUtils.format(date, YMD_HMS_FORMAT)
    }

    /**
     * 时间戳转日期
     *
     * @param ts
     * @return
     */
    def timeStamp2Date(ts: Long): String = {
        val date = new Date(ts)
        DateFormatUtils.format(date, YMD_FORMAT)
    }

    def timeStamp2DateUDF: UserDefinedFunction = udf((ts: Long) => timeStamp2Date(ts))

    /**
     * 获取两个日期间隔的天数,日期1-日期2
     *
     * @param yyyyMMdd1
     * @param yyyyMMdd2
     * @return
     */
    def getYmdGapDays(yyyyMMdd1: String, yyyyMMdd2: String): Int = {
        val date1 = DateUtils.parseDate(yyyyMMdd1, YMD_FORMAT)
        val date2 = DateUtils.parseDate(yyyyMMdd2, YMD_FORMAT)
        ((date1.getTime - date2.getTime) / this.DAY_MILLI_SECONDS).toInt
    }

    /**
     * 获取两个日期间隔的天数的绝对值,math.abs(日期1-日期2)
     *
     * @param yyyyMMdd1
     * @param yyyyMMdd2
     * @return
     */
    def getYmdGapAbsDays(yyyyMMdd1: String, yyyyMMdd2: String): Int = {
        math.abs(getYmdGapDays(yyyyMMdd1, yyyyMMdd2))
    }

    /**
     * 获取给定日期对应0点0分0秒的时间戳.
     *
     * @param yyyyMMdd
     * @return
     */
    def getDayStartTsFromYMD(yyyyMMdd: String): Long = {
        val date = DateUtils.parseDate(yyyyMMdd, YMD_FORMAT)
        date.getTime
    }

    /**
     * 获取时间字符串对应的时间戳，时间字符串格式：yyyy-MM-dd HH:mm:ss
     *
     * @param yMdHms
     * @return
     */
    def getTsFromYMdHms(yMdHms: String): Long = {
        val date = DateUtils.parseDate(yMdHms, YMD_HMS_FORMAT)
        date.getTime
    }

    /**
     * 获得当前N天前的日期
     *
     * @param n N天前
     * @return
     */

    def getFrontDay(n: Int): String = {
        val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
        val cal: Calendar = Calendar.getInstance()
        cal.add(Calendar.DATE, -n)
        val yesterday = dateFormat.format(cal.getTime)
        yesterday
    }

    /**
     * 根据dateStr，获取N天前日期
     *
     * @param dateStr
     * @param n
     * @return
     */
    def getFrontDay(dateStr: String, n: Int): String = {
        val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
        val date = dateFormat.parse(dateStr)
        val cal: Calendar = Calendar.getInstance()
        cal.setTime(date)
        cal.add(Calendar.DATE, -n)
        val BeforeDay = dateFormat.format(cal.getTime)
        BeforeDay
    }

    /**
     * 返回给定日期字符串是星期几
     * 1-7分别代表周一到周日
     *
     * @param dateStr yyyyMMdd
     * @return
     */
    def getDayOfWeek(dateStr: String): Double = {
        val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
        val date = dateFormat.parse(dateStr)
        val cal: Calendar = Calendar.getInstance()
        cal.setTime(date)
        val dayOfWeek = if (cal.get(Calendar.DAY_OF_WEEK) - 1 == 0) 7 else cal.get(Calendar.DAY_OF_WEEK) - 1
        dayOfWeek.toDouble
    }

    /**
     * 获取10位时间戳
     *
     * @return
     */
    def getUnixTimeStamp: Long = get13TimeStamp / 1000

    def get13TimeStamp: Long = Calendar.getInstance().getTimeInMillis

    /**
     * 将给定的日期转成yyyyMMdd的格式
     */
    def date2yyyyMMdd(date: String): String = {
        "/year=" + date.slice(0, 4) + "/month=" + date.slice(4, 6) + "/day=" + date.slice(6, 8)
    }

    /**
     * 日期做分割
     */
    def dateSplit(date: String): String = yyyyMMddSplit(date)

    def yyyyMMddSplit(date: String): String = {
        date.slice(0, 4) + "-" + date.slice(4, 6) + "-" + date.slice(6, 8)
    }

    /**
     * 根据时间戳返回小时值.
     *
     * @return
     */
    def udfGetHourFromTs(): UserDefinedFunction = udf((eventTime: Long) => TimeUtils.getHourFromTs(eventTime))


    /**
     * 给定两个日期字符串之间的日期差
     */
    def dateDiff(startDate: String, endDate: String, pattern: String = "yyyyMMdd"): Int = {
        val format = new java.text.SimpleDateFormat(pattern)
        val diff = format.parse(startDate).getTime - format.parse(endDate).getTime
        val days = TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS)
        math.abs(days.toInt)
    }

    /**
     * 两个日期之间的天数
     *
     * @example daysBetweenDates(20220101, 20220115)=15
     */
    def daysBetweenDate(date1: String, date2: String, pattern: String = "yyyyMMdd"): Long = {
        val formatter = DateTimeFormatter.ofPattern(pattern)
        val localDate1 = LocalDate.parse(date1, formatter)
        val localDate2 = LocalDate.parse(date2, formatter)
        ChronoUnit.DAYS.between(localDate1, localDate2).toInt + 1
    }

    /**
     * 给定时间范围内的所有时间串
     */
    def dateSeqBetweenStartAndEnd(startDate: String, endDate: String): Array[String] = {
        dateSeqBetweenEndAndDiff(endDate, dateDiff(startDate, endDate)).toArray.sorted
    }

    def dateSeqBetweenEndAndDiff(endDate: String, dateDiff: Int): ArrayBuffer[String] = {
        val result = new ArrayBuffer[String]()
        Range(0, dateDiff + 1).foreach(d => {
            result.append(getFrontDay(endDate, d))
        })
        result
    }

    def getCurrentTimestamp: String = {
        val now: Date = new Date()
        val dateFormat: SimpleDateFormat = new SimpleDateFormat(YMD_HMS_FORMAT)
        val date = dateFormat.format(now)
        date
    }
}
