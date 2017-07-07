package com.analysis.main

import com.analysis.util.{HdfsUtil, ImeiUtil}
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
    val imeisDF = ImeiUtil.getAllImeis(spark, null, null, null)

    val imeisRDD = imeisDF.rdd.map(t => {
      //t(0)  id
      val imei = t(1).toString
      val imeiid = t(2).toString

//      (imei, imeiid)
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
      (arr(4),l)
    })


    val cpz = sc.textFile("/test/fenxi/cpzCheck.log").map(l => {
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
//      println(imei1)
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
/*    println("cpz num=" + cpz.count())
    imeisRDD.take(10).foreach(println)*/


    val arr = sc.textFile("/test/fenxi/arrCheck.log").map(l => {
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

    println("cpz num=" + cpz.count() + ", arr num=" + arr.count())

    val result = cpz.join(arr).map(l => {
      val set1 = l._2._1
      val set2 = l._2._2
/*      println("set1=" + set1)
      println("set2=" + set2)*/
      val interSet = set1 & set2
      (l._1,interSet.size)
/*      if (set2!=null) {

      } else {
        l._1 + "," + 0
      }*/

/*      println("insertSet=" + interSet)
      println()*/

    }).join(imeisFormat).flatMap(f=>{
      val list = ArrayBuffer[(String,Int)]()
      val imeis = f._2._2
      val num = f._2._1
      for(i<-imeis)
        list+=((i,num))
      list
    }).join(data).map(l => {
      println(l._2._2 + "," + l._2._1)
      l._2._2 + "," + l._2._1

    })
    println(result.count())



    result.repartition(1).saveAsTextFile("/test/fenxi/cpz-arr")
//      .repartition(1).saveAsTextFile("/test/fenxi/1.csv")

    sc.stop()
    spark.stop()

  }
}
