package com.analysis.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  *
  * created by Trigl at 2017-04-26 10:10
  */
class SparkUtil {

  @transient private var instance: SparkSession = _

  def getInstance(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession
        .builder()
        .config(sparkConf)
        .enableHiveSupport()
        .getOrCreate()
    }
    instance
  }
}
