package com.xm.sdalg.commons

import java.nio.charset.Charset
import java.security.MessageDigest

import com.google.common.hash.Hashing.murmur3_32
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object HashUtils extends Serializable {

    /**
     * 判断是否是16进制字符串.
     *
     * @param str
     * @return
     */
    def isHexString(str: String): Boolean = {
        if (str == null || str.length == 0) {
            return false
        }
        var result = true
        for (i <- 0 until str.length if result) {
            val c = str.charAt(i)
            if (!((c >= '0' && c <= '9') ||
                (c >= 'a' && c <= 'f') ||
                (c >= 'A' && c <= 'F'))) {
                result = false
            }
        }
        result
    }

    /**
     * 字符串做Md5Hash.
     *
     * @param content
     * @return
     */
    def hashMD5(content: String): String = {
        val md5 = MessageDigest.getInstance("MD5")
        val encoded = md5.digest((content).getBytes)
        encoded.map("%02x".format(_)).mkString
    }

    /**
     * 将字符串在给定的key下做hash
     *
     * @param hashNum hash的key
     * @return
     */
    def hashStringUdf(hashNum: Int): UserDefinedFunction = {
        udf((col: String) => {
            if (col != null)
                (Math.abs(murmur3_32().hashString(col, Charset.defaultCharset()).asInt()) % hashNum).toFloat
            else
                0F
        })
    }

    def hashStringUdf(hashNum: Int, col: String): Int = {
        Math.abs(murmur3_32().hashString(col, Charset.defaultCharset()).asInt()) % hashNum
    }

}
