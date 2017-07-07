package com.lanny.analysistest

import org.apache.spark.Partitioner

/**
  * 按照后缀数字确定对应分区
  * created by Trigl at 2017-06-26 17:30
  */
class NumberPartitioner(numParts: Int) extends Partitioner {
  override def numPartitions: Int = numParts

  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[String]
    val num = k.substring(k.lastIndexOf("-") + 1)
    num.toInt
  }

  override def equals(other: Any): Boolean = other match {
    case custom: NumberPartitioner =>
      custom.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions
}
