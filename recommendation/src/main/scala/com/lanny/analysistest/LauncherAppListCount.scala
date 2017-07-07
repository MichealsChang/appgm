package com.lanny.analysistest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}

/**
  * 统计桌面所有APP的点击数
  * created by Trigl at 2017-05-12 11:18
  */
object LauncherAppListCount {

  def main(args: Array[String]) {

    if (args.length == 0) {
      System.err.println("参数异常")
      System.exit(1)
    }

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val sparkConf = new SparkConf()
      .setAppName("LauncherAppListCount_" + args(0))
    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql


    sql("use arrival")

    // 原始数据
    val data = sql("select applist from base_launcher_click")
      .map(r => {
        val pkgs = r.getSeq[Row](0).map(r => (r.getString(0), r.getInt(3)))
        pkgs
      }).flatMap(r => {
      var lst = List[(String, Long)]()
      for (pkg <- r) {
        lst.::=(pkg._1,pkg._2.toLong)
      }
      lst
    }).rdd.repartition(1).reduceByKey((a,b) => {
      a+b
    }).map(r => {
      (r._2, r._1)
    }).sortByKey(false).map(r => {
      (r._2,r._1)
    })

    val result = data.map(l => l._1 + "," + l._2)
    result.saveAsTextFile("/analyze/appCompare/" + args(0))

    spark.stop()

  }


}
