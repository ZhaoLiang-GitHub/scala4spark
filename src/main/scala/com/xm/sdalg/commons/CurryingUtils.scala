package com.xm.sdalg.commons

import com.xm.mib.video.common.util.KryoUtils
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD

/**
 * rdd柯里化函数
 */
object CurryingUtils extends Serializable {
    implicit class CurriedRDDFunctions[K, V](rdd: RDD[(K, V)]) {
        /**
         * 输入的rdd,是需要写入HBASE的最后的结果，key是rowkey，value是需要写入的值，值的对象是一个自定义的类
         * 实现将key、value转换成HBASE的rowkey、value，并序列化
         * 返回的是RDD[(ImmutableBytesWritable, KeyValue)]
         */
        def rdd2Hbaserdd(family: String, qualifier: String): RDD[(ImmutableBytesWritable, KeyValue)] =
            rdd
                .map({ case (k, v) =>
                    val rowKey: Array[Byte] = Bytes.toBytes(k.asInstanceOf[String])
                    (new ImmutableBytesWritable(rowKey),
                        new KeyValue(rowKey, Bytes.toBytes(family), Bytes.toBytes(qualifier), KryoUtils.writeObjectToByteArray(v)))
                })
    }
}
