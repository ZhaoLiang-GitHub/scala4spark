package com.xm.sdalg.commons

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import org.apache.hadoop.io.Writable

object WritableSerDerUtils {

    /**
     * 将一个实现了Writable接口的对象序列化为字节数组。
     *
     * @param writable 要被序列化的Writable对象。
     * @return 返回该对象的字节数组形式。
     */
    def serialize(writable: Writable): Array[Byte] = {
        // 初始化ByteArrayOutputStream来捕获序列化后的数据。
        val out = new ByteArrayOutputStream()

        // 使用DataOutputStream包装ByteArrayOutputStream，以便以平台无关的方式写入数据。
        val dataOut = new DataOutputStream(out)

        // 使用Writable对象的write方法进行序列化。
        writable.write(dataOut)

        // 关闭DataOutputStream。
        dataOut.close()

        // 将ByteArrayOutputStream内容转换为字节数组并返回。
        out.toByteArray
    }

    /**
     * 将字节数组反序列化为Writable对象。
     *
     * @param bytes    被序列化对象的字节数组表示。
     * @param writable 一个要被填充反序列化数据的Writable实例。此实例应与原始序列化对象的类型相同。
     * @return 返回填充了反序列化数据的Writable对象。
     */
    def deserialize(bytes: Array[Byte], writable: Writable): Writable = {
        // 使用ByteArrayInputStream从提供的字节数组中读取数据。
        val in = new ByteArrayInputStream(bytes)

        // 使用DataInputStream包装ByteArrayInputStream，以便以平台无关的方式读取数据。
        val dataIn = new DataInputStream(in)

        // 使用Writable对象的readFields方法填充数据。
        writable.readFields(dataIn)

        // 关闭DataInputStream。
        dataIn.close()

        // 返回填充了数据的Writable对象。
        writable
    }
}
