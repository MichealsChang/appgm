package com.lanny.analysistest

import java.util.Calendar

import com.lanny.analysistest.util.{HdfsUtil, ImeiUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{HashPartitioner, SparkConf}

import scala.collection.mutable.{ArrayBuffer, Map, Set}

/**
  * 渠道纬度到达匹刷机
  */
object GetUserAppDataByDayRange {
  val RATING_OF_FIRST_ADD: Double = 1
  val RATING_OF_EXIST_ONE_DAY: Double = 2
  val RATING_OF_EXIST_SEVEN_DAY: Double = 3
  val RATING_OF_EXIST_THIRTY_DAY: Double = 4
  val RATING_OF_REMOVE: Double = -1


  def main(args: Array[String]) {
    //参数   20170412
    if (args == null || args.length < 1) {
      System.err.println("参数异常")
      System.exit(1)
    }

    val year = args(0).substring(0, 4)
    val month = args(0).substring(4, 6)
    val day = args(0).substring(6, 8)
    val cal = Calendar.getInstance
    cal.set(year.toInt, month.toInt - 1, day.toInt)

    //添加对前一天的校验
    val yesterday = Calendar.getInstance
    yesterday.setTime(cal.getTime)
    yesterday.add(Calendar.DAY_OF_MONTH, -1)

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    System.setProperty("spark.network.timeout", "300s")
    val sparkConf = new SparkConf().setAppName("GetUserAppDataByDayRange_" + args(0))
    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
    println("Start " + "GetUserAppDataByDayRange_" + args(0))

    import spark.sql

    val imeisRDD = ImeiUtil.getAllImeis_V2(spark, year, month, day)

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

    // 今天推荐的原始数据
    sql("use arrival")
    val arriveDF = sql("select " +
      "(case when (firstimei != ' ') then firstimei when (secondimei != ' ') then secondimei when (thirdimei != ' ') then thirdimei else null end) as imei, " +
      "applist, accepttimestamp from  base_arrival_new " +
      "where year=" + year + " and month=" + month + " and day=" + day)
    val arriveRDD = arriveDF.rdd
      .filter(r => r.get(2) != null) // 确保服务器时间不为空
      .map(x => {
      val imei = x.getString(0)
      val apps = x.getSeq[Row](1).map(r => r.getString(1))
      val accepttime = x.getDouble(2).toLong
      (imei, (apps, accepttime))
    })

    // 前一天汇总的结果数据
    val yes_yearmonthday = getYearMonthDay(yesterday)
    val lastDataDF = sql("select * from mid_recommend_data where " +
      "year=" + yes_yearmonthday(0) + " and month=" + yes_yearmonthday(1) + " and day=" + yes_yearmonthday(2))
    val lastRDD = lastDataDF.rdd
      .filter(r => r.getString(0) != null)
      .map(x => {
        val imei = x.getString(0)
        val apps = x.getSeq[String](1)
        val accepttime = x.getLong(2)
        (imei, (apps, accepttime))
      })

    // 前一天的推荐打分数据
    val preRecommendDF = sql("select * from mid_recommend_rating where " +
      "year=" + yes_yearmonthday(0) + " and month=" + yes_yearmonthday(1) + " and day=" + yes_yearmonthday(2))
    val preRecommendRDD = preRecommendDF.rdd.map(f => {
      //imei|pkg|rating|timestamp
      try {
        val imei = f.getString(0)
        val pkg = f.getString(1)
        val rating = f.getDouble(2)
        val timestamp = f.getLong(3)
        (imei, (pkg, timestamp, rating))
      } catch {
        case e: Exception =>
          e.printStackTrace()
          null
      }
    }).join(imeisRDD)
      .map(f => {
        val ratingData = f._2._1
        val imeiid = f._2._2
        (imeiid, ratingData)
      }).combineByKey(
      (v: (String, Long, Double)) => {
        var appMap = Map[String, (Long, Double)]()
        appMap += (v._1 ->(v._2, v._3))
        appMap
      }, (acc: Map[String, (Long, Double)], v: (String, Long, Double)) => {
        acc += (v._1 ->(v._2, v._3))
        acc
      }, (acc1: Map[String, (Long, Double)], acc2: Map[String, (Long, Double)]) => {
        acc1 ++ acc2
      })

    // 所有的数据
    val allRDD = arriveRDD.union(lastRDD).partitionBy(new HashPartitioner(1000))
      .join(imeisRDD)
      .map(f => {
        try {
          //val list = ArrayBuffer[((String,String),Long)]()
          val arriveData = f._2._1
          val imeiid = f._2._2
          val apps = arriveData._1
          val accepttime = arriveData._2
          //          val appSet = Set(apps.mkString(";"))
          val appSet = Set[String]()
          for (a <- apps) {
            appSet.add(a)
          }

          (imeiid, (appSet, accepttime))
        } catch {
          case e: Exception =>
            e.printStackTrace()
            null
        }
      }).filter(_ != null)
      .map(f => {
        (f._1, f._2 :: Nil)
      }).reduceByKey(_ ::: _)
      .map(f => {
        val imeiid = f._1
        val list = f._2.sortBy(_._2)
        val newLastData = list(list.length - 1)
        var addMap = Map[String, Long]()
        var removeMap = Map[String, Long]()
        for ((v, i) <- list.zipWithIndex) {
          if (i > 0) {
            //首次的不做处理
            val addApps = v._1 -- list(i - 1)._1
            val removeApps = list(i - 1)._1 -- v._1
            if (!addApps.isEmpty) {
              for (s <- addApps) {
                addMap += (s -> v._2)
              }

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

        var finalAddMap = addMap -- interKeys
        var finalRemoveMap = removeMap -- interKeys
        for (s <- interKeys) {
          val lastAddTime = addMap.get(s).get
          val lastremoveTime = removeMap.get(s).get
          if (lastAddTime > lastremoveTime)
            finalAddMap += (s -> lastAddTime)
          else
            finalRemoveMap += (s -> lastremoveTime)
        }

        (imeiid, (newLastData, finalAddMap, finalRemoveMap))
      })


    // 新的推荐打分数据
    var resultRDD: RDD[(String, ((Set[String], Long), Map[String, (Double, Long)]))] = null
    resultRDD = allRDD.leftOuterJoin(preRecommendRDD)
      .map(f => {
        val imeiid = f._1
        val arrData = f._2._1
        val lastData = arrData._1
        val addMap = arrData._2
        val removeMap = arrData._3
        val preRecommendData = f._2._2.orNull
        var ratingData = Map[String, (Double, Long)]()
        if (preRecommendData != null) {
          // 与昨天相比不增不减的软件打分
          val leftData = preRecommendData -- removeMap.keys
          for (left <- leftData) {
            val pkg = left._1
            val time = left._2._1
            val prerating = left._2._2
            if (prerating == RATING_OF_REMOVE) //之前删除的保留
              ratingData += getRating(RATING_OF_REMOVE, pkg, time)
            else {
              val lasttime = lastData._2
              val interval = (lasttime - time).toDouble / (1000 * 60 * 60 * 24)
              ratingData += getRating(interval, pkg, time)
            }
          }
        }
        //与昨天相比增加的软件打分
        for (a <- addMap) {
          val pkg = a._1
          val firsttime = a._2
          val lasttime = lastData._2
          val interval = (lasttime - firsttime).toDouble / (1000 * 60 * 60 * 24)
          ratingData += getRating(interval, pkg, firsttime)
        }
        //与昨天相比卸载的软件打分
        for (a <- removeMap) {
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

    HdfsUtil.checkDirExist(HdfsUtil.HDFS_PATH, spark.sparkContext, HdfsUtil.RECOMMEND_LAST_DIR + "/" + year + "/" + month + "/" + day)
    newLastRDD.saveAsTextFile(HdfsUtil.RECOMMEND_LAST_DIR + "/" + year + "/" + month + "/" + day)

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

    HdfsUtil.checkDirExist(HdfsUtil.HDFS_PATH, spark.sparkContext, HdfsUtil.RECOMMEND_RATING_DIR + "/" + year + "/" + month + "/" + day)
    ratingRDD.saveAsTextFile(HdfsUtil.RECOMMEND_RATING_DIR + "/" + year + "/" + month + "/" + day)

    spark.stop()

    val rt = Runtime.getRuntime()
    val command = "nohup /data/install/spark-2.0.0-bin-hadoop2.7/bin/spark-submit --master spark://hxf:7077  --executor-memory 20G --total-executor-cores 20  --conf spark.storage.memoryFraction=0.1 --conf spark.shuffle.memoryFraction=0.8 --conf spark.sql.shuffle.partitions=2000 --name Data-Analyze --supervise --class com.lanny.analysistest.AppRecommender  --jars /home/hadoop/jar/fastjson-1.2.1.jar /home/hadoop/jar/recommendation.jar " + args(0) + " > /home/hadoop/logs/appRecommender.log &" +
      " nohup /data/install/spark-2.0.0-bin-hadoop2.7/bin/spark-submit --master spark://hxf:7077  --executor-memory 20G --total-executor-cores 20 --conf spark.sql.shuffle.partitions=2000 --name Data-Analyze --supervise --class com.lanny.analysistest.FolderRecommender  --jars /home/hadoop/jar/fastjson-1.2.1.jar /home/hadoop/jar/recommendation.jar " + args(0) + " > /home/hadoop/logs/folderRecommender.log &" +
      " nohup /data/install/spark-2.0.0-bin-hadoop2.7/bin/spark-submit  --master spark://hxf:7077  --executor-memory 20G --total-executor-cores 40 --conf spark.default.parallelism=1000 --conf spark.storage.memoryFraction=0.1 --conf spark.shuffle.memoryFraction=0.8     --class com.lanny.analysistest.ItemBasedRecommendation  --jars /home/hadoop/jar/fastjson-1.2.1.jar  /home/hadoop/jar/recommendation.jar " + args(0) + " > /home/hadoop/logs/ItemBasedRecommendation.log &"
    val cmd = Array("/bin/sh", "-c", command)
    try {

      val proc = rt.exec(cmd)
      // 获取进程的标准输入流
      proc.waitFor()
      println("提交Recommender任务")
      println(command)

    } catch {
      case e: Exception =>
        println(args(0) + "提交失败：" + e.getMessage)
        e.printStackTrace()
    }
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