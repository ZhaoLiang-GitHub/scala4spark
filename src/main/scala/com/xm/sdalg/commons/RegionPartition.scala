package com.xm.sdalg.commons

import org.apache.spark.Partitioner

class RegionPartition(num: Int = 1296) extends Partitioner {

    override def numPartitions: Int = num

    override def getPartition(key: Any): Int = {
        val s = String.valueOf(key).trim.substring(0, 2).trim
        if (s != null && s.nonEmpty && !s.equals("")) {
            Integer.valueOf(s.trim, 36)
        } else {
            throw new Exception("key is :" + String.valueOf(key))
        }
    }

}

