package com.lanny.analysistest

import com.lanny.analysistest.util.ImeiUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks

/**
  * 统计桌面点击的APP
  * created by Trigl at 2017-05-12 11:18
  */
object LauncherAppCount {

  def main(args: Array[String]) {

    // AA8D3A60 点传装机
    if (args.length == 0) {
      System.err.println("参数异常")
      System.exit(1)
    }

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val sparkConf = new SparkConf()
      .setAppName("LauncherAppCount_" + args(0))
    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    import spark.sql
    import spark.implicits._

    sql("use arrival")


    val imeisRDD = ImeiUtil.getAllImeis_V2(spark, null, null, null)

    // 保留所有imei
    val imeisFormat = imeisRDD.map(f => {
      val imeiid = f._2
      val imei = f._1
      (imeiid, imei :: Nil)
    }).reduceByKey((a, b) => {
      a ::: b
    })

    // 原始数据
    val data = sql("select imeis, applist from base_launcher_click")
      .map(r => {
        // Scala没有continue，使用这种方式中断
        val loop = new Breaks
        var count = 0 // 点击次数

        val imei = r.getSeq[String](0)(0)

        val pkgs = r.getSeq[Row](1).map(r => (r.getString(1), r.getInt(3)))
        loop.breakable {
          for (p <- pkgs) {
            val crc = p._1
            val clicktimes = p._2
            if (args(0).equalsIgnoreCase(crc)) {
              count = clicktimes
              loop.break()
            }
          }
        }

        (imei, count)
      }).filter(_._2 != 0).rdd.join(imeisRDD)
      .map(line => {
        val imeiid = line._2._2
        val clicktimes = line._2._1
        (imeiid, clicktimes)
      }).reduceByKey((a,b) => {
      a + b
    }).join(imeisFormat).map(line => {
      val list = ArrayBuffer[(String,Int)]()
      val clicktimes = line._2._1
      val imeis = line._2._2

      var formatImeis = imeis.mkString(",")

      if (imeis.size == 2) {
        formatImeis += ","
      }
      if (imeis.size == 1) {
        formatImeis += ",,"
      }

      (formatImeis, clicktimes)
    })

    val result = data.map(l => l._1 + "," + l._2)
    result.repartition(1).saveAsTextFile("/analyze/" + args(1))

    spark.stop()

  }


}
