package com.lanny.analysistest

import scala.collection.mutable.Set
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import com.lanny.analysistest.util.ImeiUtil
import java.util.Calendar

import com.lanny.analysistest.util.HdfsUtil
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.{Level, Logger}

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

/**
  * 初始化推荐系统
  */
object GetUserAppDataByDayRangeInit {
  val RATING_OF_FIRST_ADD: Double = 1
  val RATING_OF_EXIST_ONE_DAY: Double = 2
  val RATING_OF_EXIST_SEVEN_DAY: Double = 3
  val RATING_OF_EXIST_THIRTY_DAY: Double = 4
  val RATING_OF_REMOVE: Double = -1

  def main(args: Array[String]) {

    //参数   20170603
    if (args == null || args.length < 1) {
      System.err.println("参数异常")
      System.exit(1)
    }

    val year = args(0).substring(0, 4)
    val month = args(0).substring(4, 6)
    val day = args(0).substring(6, 8)

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    // 设置序列化方式
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    System.setProperty("spark.network.timeout", "300s")
    // 初始化SparkSession，用于连接Hive
    val sparkConf = new SparkConf().setAppName("GetUserAppDataByDayRangeInit_" + args(0))
    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
    println("Start " + "GetUserAppDataByDayRangeInit_" + args(0))

    import spark.sql

    val imeisRDD = ImeiUtil.getAllImeis_V2(spark, "2017", "06", "08")
//    val imeisRDD = ImeiUtil.getAllImeis_V2(spark, year, month, day)

    // hdfs中留一个即可
    val imeisFormat = imeisRDD.map(f => {
      val imeiid = f._2
      val imei = f._1
      (imeiid, imei)
    }).reduceByKey((a, b) => {
        if (a > b)
          a
        else
          b
      })

    sql("use arrival")
    val arriveDF = sql("select " +
      "(case when (firstimei != ' ') then firstimei when (secondimei != ' ') then secondimei when (thirdimei != ' ') then thirdimei else null end) as imei, " +
      "applist, accepttimestamp from  base_arrival_new where month !='04' and month !='05' and month != '06'")
    val arriveRDD = arriveDF.rdd
      .filter(r => r.getString(0) != null)
      .map(x => {
        val imei = x.getString(0)
        val apps = x.getSeq[Row](1)
        val accepttime = if(x.get(2)!= null) x.getDouble(2).toLong else 0L
        (imei, (apps, accepttime))
      }).partitionBy(new HashPartitioner(3000)).join(imeisRDD)
      .map(f => {
        try {
          //val list = ArrayBuffer[((String,String),Long)]()
          val arriveData = f._2._1
          val imeiid = f._2._2
          val apps = arriveData._1
          val accepttime = arriveData._2
          val appSet = Set[String]()
          for (a <- apps) {
            val pkg = a.getString(1)
            appSet.add(pkg)
          }
          (imeiid, (appSet, accepttime))
        } catch {
          case e: Exception =>
            null
        }
      }).filter(_ != null)


    val unionRDD: RDD[(String, (Set[String], Long))] = arriveRDD


    val allRDD = unionRDD //lastRDD.union()
      .map(f => {
      (f._1, f._2 :: Nil) // 改为一个List
    }).reduceByKey(_ ::: _)
      .map(f => {
        // 得到最近变化的app列表
        val imeiid = f._1
        val list = f._2.sortBy(_._2) // 按时间排序
        val newLastData = list(list.length - 1) // 最后一次的数据
        var addMap = Map[String, Long]()
        var removeMap = Map[String, Long]()
        for ((v, i) <- list.zipWithIndex) {
          if (i > 0) {
            //首次的不做处理
            val addApps = v._1 -- list(i - 1)._1 // 增加的app
            val removeApps = list(i - 1)._1 -- v._1 // 减少的app
            if (!addApps.isEmpty) {
              for (s <- addApps)
                addMap += (s -> v._2)
            }
            if (!removeApps.isEmpty) {
              for (s <- removeApps)
                removeMap += (s -> v._2)
            }
          }
        }
        val addKeys = addMap.keySet
        val removeKeys = removeMap.keySet
        val interKeys = addKeys & removeKeys

        var fianlAddMap = addMap -- interKeys
        var finalRemoveMap = removeMap -- interKeys
        for (s <- interKeys) {
          val lastAddTime = addMap.get(s).get
          val lastremoveTime = removeMap.get(s).get
          if (lastAddTime > lastremoveTime)
            fianlAddMap += (s -> lastAddTime)
          else
            finalRemoveMap += (s -> lastremoveTime)
        }
        (imeiid, (newLastData, fianlAddMap, finalRemoveMap))
      })


    val resultRDD = allRDD
      .map(f => {
        val imeiid = f._1
        val arrData = f._2
        val lastData = arrData._1
        val addMap = arrData._2
        val removeMap = arrData._3
        var ratingData = Map[String, (Double, Long)]()
        for (a <- addMap) {
          //新增的lastData里有
          val pkg = a._1
          val firsttime = a._2
          val lasttime = lastData._2
          val interval = (lasttime - firsttime).toDouble / (1000 * 60 * 60 * 24) // 新增app时间和最近上报app时间间隔
          ratingData += getRating(interval, pkg, firsttime)
        }
        for (a <- removeMap) {
          //删除
          val pkg = a._1
          val removetime = a._2
          ratingData += (pkg ->(RATING_OF_REMOVE, removetime))
        }

        (imeiid, (lastData, ratingData))
      }).join(imeisFormat)
      .map(f => {
        val imei = f._2._2
        val data = f._2._1
        (imei, data)
      })


    val newLastRDD = resultRDD.map(f => {
      val imei = f._1
      val newlastData = f._2._1
      val time = newlastData._2
      imei + "|" + newlastData._1.mkString(";") + "|" + time
    })
    newLastRDD.saveAsTextFile(HdfsUtil.RECOMMEND_LAST_DIR + "/" + year +"/" + month + "/" + day)

    val ratingRDD = resultRDD.flatMap(f => {
      val list = ArrayBuffer[String]()
      val imei = f._1
      val ratingData = f._2._2
      for (r <- ratingData) {
        val pkg = r._1
        val rating = r._2._1
        val timestamp = r._2._2
        list += (imei + "|" + pkg + "|" + rating + "|" + timestamp)
      }
      list
    })
    ratingRDD.saveAsTextFile(HdfsUtil.RECOMMEND_RATING_DIR + "/" + year +"/" + month + "/" + day)

    spark.stop()


  }

  def getRating(interval: Double, pkg: String, firsttime: Long): (String, (Double, Long)) = {
    //var s:(String ,(Double,Long)) = null
    if (interval < 1)
      (pkg, (RATING_OF_FIRST_ADD, firsttime))
    else if (interval >= 1 && interval < 7)
      (pkg, (RATING_OF_EXIST_ONE_DAY, firsttime))
    else if (interval >= 7 && interval < 30)
      (pkg, (RATING_OF_EXIST_SEVEN_DAY, firsttime))
    else
      (pkg, (RATING_OF_EXIST_THIRTY_DAY, firsttime))
  }

  def getYearMonthDay(cal: Calendar): Array[String] = {
    val yearStr = cal.get(Calendar.YEAR).toString()
    var monthStr = ""
    if ((cal.get(Calendar.MONTH) + 1) < 10)
      monthStr = "0" + (cal.get(Calendar.MONTH) + 1)
    else
      monthStr = "" + (cal.get(Calendar.MONTH) + 1)
    var dayStr = ""
    if (cal.get(Calendar.DAY_OF_MONTH) < 10)
      dayStr = "0" + cal.get(Calendar.DAY_OF_MONTH)
    else
      dayStr = "" + cal.get(Calendar.DAY_OF_MONTH)

    Array[String](yearStr, monthStr, dayStr)
  }

}