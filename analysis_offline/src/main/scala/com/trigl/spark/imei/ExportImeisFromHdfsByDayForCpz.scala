package com.trigl.spark.imei

import java.io.File
import java.util.Date

import com.trigl.spark.utils.HdfsUtil
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.{SparkConf, SparkContext}

/**
  * *
  * 从每日装刷数据里提取imei数据到Hdfs，作为入imei库程序的输入
  */
object ExportImeisFromHdfsByDayForCpz {

  val sdf = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.SSS")

  def main(args: Array[String]) {
    //每日装刷imei数据/changmi/cpz_data/2016/12/14/11
    if (args == null || args.length < 1) {
      System.err.println("参数异常")
      System.exit(1)
    }

    var tmp = args(0)
    var pos = tmp.lastIndexOf("/")
    val hour = tmp.substring(pos + 1)

    tmp = tmp.substring(0, pos)
    pos = tmp.lastIndexOf("/")
    val day = tmp.substring(pos + 1)

    tmp = tmp.substring(0, pos)
    pos = tmp.lastIndexOf("/")
    val month = tmp.substring(pos + 1)

    tmp = tmp.substring(0, pos)
    pos = tmp.lastIndexOf("/")
    val year = tmp.substring(pos + 1)
    val hdfs_output = HdfsUtil.IMEIS_CPZ_DIR + "/" + year + "/" + month + "/" + day + "/" + hour + "/" + "imeis" + year + month + day + hour

    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    System.setProperty("spark.network.timeout", "300s")
    System.setProperty("spark.storage.memoryFraction", "0.1")
    val sparkConf = new SparkConf().setAppName("ExportImeisFromHdfsByDayForCpz_" + args(0))
    val sc = new SparkContext(sparkConf)

    val cpzData = sc.textFile(args(0))
    cpzData.map(f => {
      try {
        val arr = f.split("&")
        if (arr != null && arr.length > 1)
          arr(1) // imei列表
        else
          null
      } catch {
        case e: Exception =>
          null
      }
    }).filter(f => {
      if (f != null)
        true
      else
        false
    })
      .saveAsTextFile(hdfs_output)

    sc.stop()
    println(" into imei start time:" + sdf.format(new Date()))
    val local = "/data/imeis/imeis_cpz/" + year + "/" + month + "/" + day + "/" + hour + "/"
    val filename = hdfs_output.substring(hdfs_output.lastIndexOf("/") + 1)
    val dir = new File(local)
    if (!dir.isDirectory()) {
      dir.mkdirs()
    }
    println("spark任务结束，执行装刷imei入库")

    println("hdfs_output:" + hdfs_output)
    println("local+filename:" + local + filename)
    val rt = Runtime.getRuntime()
    //命令需要全路径
    val cmd = Array("/bin/sh", "-c", " /data/install/hadoop/bin/hadoop fs -cat " + hdfs_output + "/* > " + local + filename + ";"

      //执行入装刷imei库操作
      + " /data/install/jdk/bin/java -cp /home/hadoop/imei-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.imei.MergeImeiForCpz " + local + filename + " > /home/hadoop/logs/imeiCpzLog.out")

    try {
      val proc = rt.exec(cmd)
      proc.waitFor()
      proc.destroy()
      println("装刷imei入库提交结束")
    } catch {
      case e: Exception =>
        println(args(0) + "装刷imei入库提交失败：" + e.getMessage)
    }
  }

}