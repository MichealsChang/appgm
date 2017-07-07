package com.lanny.analysistest

import com.lanny.analysistest.util.{DbUtil, HdfsUtil, ImeiUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 从Mysql获取IMEI数据到HDFS
  * created by Trigl at 2017-05-23 15:49
  */
object GetImeiFromDB {

  def main(args: Array[String]) {

    // 20170523
    if (args.length == 0) {
      println("参数异常")
      System.exit(1)
    }

    val year = args(0).substring(0, 4)
    val month = args(0).substring(4, 6)
    val day = args(0).substring(6, 8)

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sparkConf = new SparkConf().setAppName("GetImeiFromDB_" + args(0))
    val sc = new SparkContext(sparkConf)
    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    val imeiDir = HdfsUtil.IMEI_ALL_BY_DAY_DIR + "/" + year + "/" + month + "/" + day

    // 首先删除目录
    HdfsUtil.deleteDir(sc, imeiDir)

    println("Get imei data from mysql")

    val maxId = ImeiUtil.allImeiMaxId(year, month, day)

    val imeis = spark.read.format("jdbc").options(
      Map("url" -> DbUtil.IMEI_DB_URL,
        "dbtable" -> ImeiUtil.IMEI_ALL_TABLE,
        "user" -> DbUtil.IMEI_DB_USERNAME,
        "password" -> DbUtil.IMEI_DB_PASSWORD,
        "driver" -> "com.mysql.jdbc.Driver",
        "fetchSize" -> "10",
        "partitionColumn" -> "id",
        "lowerBound" -> "1",
        "upperBound" -> maxId.toString(),
        "numPartitions" -> "15")).load()

    val imeisRDD = imeis.rdd.map(t => {
      //t(0)  id
      val imei = t(1).toString
      val imeiid = t(2).toString

      (imei, imeiid)
    })

    //保存到hdfs
    imeisRDD.map(f => {

      (f._1 :: f._2 :: Nil).mkString("&")
    })
      .saveAsTextFile(HdfsUtil.IMEI_ALL_BY_DAY_DIR + "/" + year + "/" + month + "/" + day)

    sc.stop()
    spark.stop()

    val rt = Runtime.getRuntime()
    val command = "nohup /data/install/spark-2.0.0-bin-hadoop2.7/bin/spark-submit  --master spark://hxf:7077  --executor-memory 15G --total-executor-cores 20   --conf spark.default.parallelism=60 --conf spark.storage.memoryFraction=0.1 --conf spark.shuffle.memoryFraction=0.8 --conf spark.sql.shuffle.partitions=1000   --class com.lanny.analysistest.GetUserAppDataByDayRange /home/hadoop/jar/recommendation.jar " + args(0) + " > /home/hadoop/logs/recommendation.log &"

    val cmd = Array("/bin/sh", "-c", command)
    try {
/*      val proc = rt.exec(cmd)
      // 获取进程的标准输入流
      proc.waitFor()
      proc.destroy()*/
      println("提交任务：")
      println(command)
    } catch {
      case e: Exception =>
        println(args(0) + "提交失败：" + e.getMessage)
        e.printStackTrace()
    }
  }
}
