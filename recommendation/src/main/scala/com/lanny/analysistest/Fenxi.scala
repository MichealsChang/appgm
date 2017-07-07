package com.lanny.analysistest

import com.lanny.analysistest.util.ImeiUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  *
  * created by Trigl at 2017-05-12 15:05
  */
object Fenxi {

  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sparkConf = new SparkConf().setAppName("FenXi")
    val sc = new SparkContext(sparkConf)

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    import spark.sql

    sql("use arrival")

    // 通过JDBC查询所有imei匹配
    val imeisRDD = ImeiUtil.getAllImeis_V2(spark, null, null, null)
      .map(t => {
      val imei = t._1
      val imeiid = t._2

      (ImeiUtil.parseLength(imei), (imei, imeiid))
    }).cache()

    // 保留所有imei
    val imeisFormat = imeisRDD.map(f => {
      val imeiid = f._2._2
      val imei = f._2._1
      (imeiid, imei::Nil)
    }).reduceByKey((a, b) => {
      a:::b
    })

    val data = sc.textFile("/test/fenxi/1.csv").map(l => {
      val arr = l.split(",")
      if (arr.length > 4) {
        (arr(4), l) // imei1
      } else {
        null
      }
    }).filter(_!=null)

    val cpz = sc.textFile("/test/fenxi/cpz").map(l => {
      val arr = l.split("\\|")
      val imei = arr(0).substring(0, 32)
      val pkgs = arr(1).split(";")
      val time = arr(2)
      val apps = ListBuffer[String]()
      if(pkgs.length != 0) {
        for (p <- pkgs) {
          val app = p.split(",")
          if (app.length > 1) {
            apps += app(1)
          }
        }
      }
      (imei, (time,apps.toSet))
    }).flatMap(l => {
      val imei1 = l._1.substring(0,16)
      val imei2 = l._1.substring(16)
      val lines = Array((imei1, l._2), (imei2, l._2))
      lines
    }).join(imeisRDD).map(l => {
      (l._2._2._2, l._2._1)
    }).reduceByKey((a,b) => {
      // 取时间比较早的
      if(a._1<b._1) {
        a
      } else {
        b
      }

    }).map(l => {
      (l._1, l._2._2)
    })

    val arr = sc.textFile("/test/fenxi/arr").map(l => {
      val arr = l.split("\\|")
      val imei = arr(0).substring(0, 32)
      val pkgs = arr(1).split(";")
      val time = arr(2)
      val apps = ListBuffer[String]()
      if(pkgs.length != 0) {
        for (p <- pkgs) {
          val app = p.substring(0, p.lastIndexOf("_"))
          apps += app
        }
      }

      (imei, (time,apps.toSet))

    }).flatMap(l => {
      val imei1 = l._1.substring(0,16)
      val imei2 = l._1.substring(16)
      val lines = Array((imei1, l._2), (imei2, l._2))
      lines
    }).join(imeisRDD).map(l => {
      (l._2._2._2, l._2._1)
    }).reduceByKey((a,b) => {
      // 取时间比较早的
      if(a._1<b._1) {
        a
      } else {
        b
      }

    }).map(l => {
      (l._1, l._2._2)
    })


    val result = cpz.join(arr).map(l => {
      val set1 = l._2._1
      val set2 = l._2._2
      val interSet = set1 & set2
      (l._1,interSet.size)

    }).join(imeisFormat).flatMap(f=>{
      val list = ArrayBuffer[(String,Int)]()
      val imeis = f._2._2
      val num = f._2._1
      for(i<-imeis)
        list+=((i,num))
      list
    }).join(data).map(l => {
      l._2._2 + "," + l._2._1
    })

    result.repartition(1).saveAsTextFile("/test/fenxi/cpz-arr")

    sc.stop()
    spark.stop()

  }
}
