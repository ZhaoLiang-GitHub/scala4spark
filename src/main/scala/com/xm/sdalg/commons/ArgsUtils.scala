package com.xm.sdalg.commons

import scala.util.Try

/**
 * 参数相关的公共函数
 */
object ArgsUtils {
    /**
     * 将输入的成对的参数，按照数字排序，然后返回参数值的数组
     */
    def args2args(args: Array[String]): Array[String] = {
        assert(args.length % 2 == 0)
        var argsMap: Map[String, String] = Map()
        Range(0, args.length / 2).foreach(i => {
            val index = i * 2
            argsMap ++= Map(args(index).replaceAll("-", "") -> args(index + 1))
        })
        val result = argsMap
            .map(ii => (Try(ii._1.toInt).getOrElse(-1), ii._2))
            .filter(ii => ii._1 != -1)
            .toArray
            .sortBy(_._1)
            .map(_._2)
        println(s"输入参数是 ${result.mkString(",")}")
        result
    }
}
