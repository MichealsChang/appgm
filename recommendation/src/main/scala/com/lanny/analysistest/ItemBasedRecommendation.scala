package com.lanny.analysistest

import breeze.numerics.sqrt
import com.alibaba.fastjson.{JSONArray, JSONObject}
import com.lanny.analysistest.util.{DbUtil, HdfsUtil, ImeiUtil, RandomUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
  * 基于物品的推荐系统
  * created by Trigl at 2017-06-20 10:54
  */
object ItemBasedRecommendation {

  val HDFS_RECOMMEND_HOME = "/changmi/recommend/result/home/"

  def main(args: Array[String]) {

    //参数   20170412
    if (args == null || args.length < 1) {
      System.err.println("参数异常")
      System.exit(1)
    }

    val year = args(0).substring(0, 4)
    val month = args(0).substring(4, 6)
    val day = args(0).substring(6, 8)

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    System.setProperty("spark.sql.crossJoin.enabled", "true") // 允许join笛卡尔积
    System.setProperty("spark.kryoserializer.buffer.max", "128")
    val sparkConf = new SparkConf()
      .setAppName("ItemBasedRecommendation_" + args(0))
      .set("spark.rpc.netty.dispatcher.numThreads", "2") // 预防RpcTimeoutException
    val spark = SparkSession
        .builder()
        .config(sparkConf)
        .enableHiveSupport()
        .getOrCreate()

    import spark.sql
    sql("use arrival")

    // 通过JDBC查询所有imei匹配
    val imeisRDD = ImeiUtil.getAllImeis_V2(spark, year, month, day)

    val preRecommendDF = sql("select * from mid_recommend_rating where " +
      "year=" + year + " and month=" + month + " and day=" + day +
      " and rating > 0")
    val preRecommendRDD = preRecommendDF.rdd.map(f => {
      //imei|pkg|rating|timestamp
      try {
        val imei = f.getString(0)
        val pkg = f.getString(1)
        val rating = f.getDouble(2)
        (imei, pkg, rating)
      } catch {
        case e: Exception =>
          e.printStackTrace()
          null
      }
    })

    val sim = similarity(preRecommendRDD, 20)
    val rmd = recommend(sim, preRecommendRDD, 20)
    // 需要推荐的APP包名，作为广播变量
    val rmdApp = DbUtil.getRecommendApp(spark).rdd.map(r => r.getString(0)).collect()
    val broadcastApp = spark.sparkContext.broadcast(rmdApp)

    val result = genDetail(spark, imeisRDD, rmd, broadcastApp)
    // 先删除HDFS的目录
    HdfsUtil.deleteDir(spark.sparkContext, HDFS_RECOMMEND_HOME)
    result.map(r => r._1 + "|" + r._2).repartition(30).saveAsTextFile(HDFS_RECOMMEND_HOME)

    spark.stop()

    val rt = Runtime.getRuntime()
    val command = "nohup /data/install/spark-2.0.0-bin-hadoop2.7/bin/spark-submit --master spark://hxf:7077  --executor-memory 20G --total-executor-cores 8  --conf spark.default.parallelism=1000 --supervise --class com.lanny.analysistest.ResultToDB --jars /home/hadoop/jar/fastjson-1.2.1.jar /home/hadoop/jar/recommendation.jar  " + args(0) + " item-based > /home/hadoop/logs/ItembasedResultToDB.log &"

    val cmd = Array("/bin/sh", "-c", command)
    try {
      val proc = rt.exec(cmd)
      // 获取进程的标准输入流
      proc.waitFor()
      proc.destroy()
      println("提交任务：")
      println(command)
    } catch {
      case e: Exception =>
        println(args(0) + "提交失败：" + e.getMessage)
        e.printStackTrace()
    }
  }

  /**
    * 计算物品相似度
    *
    * @param user_rdd
    * @param filterNum 只取最终filterNum个结果
    * @return
    */
  def similarity(user_rdd: RDD[(String, String, Double)], filterNum: Int): RDD[(String, String, Double)] = {

    // 0、数据做准备
    val user_rdd2 = user_rdd.map(f => (f._1, f._2)).sortByKey()
    user_rdd2.cache()

    // 1、（用户：物品） 笛卡尔积 （用户：物品） => 物品：物品组合
    val user_rdd3 = user_rdd2.join(user_rdd2)
    val user_rdd4 = user_rdd3.map(data => (data._2, 1))

    // 2、（物品：物品）：频次
    val user_rdd5 = user_rdd4.reduceByKey((x, y) => x + y)

    // 3、对角矩阵
    val user_rdd6 = user_rdd5.filter(f => f._1._1 == f._1._2)

    // 4、非对角矩阵
    val user_rdd7 = user_rdd5.filter(f => f._1._1 != f._1._2)

    // 5、计算同现相似度（物品1：物品2：同现频次）
    // 物品1：（（物品1：物品2：同现频次），物品1频次）
    val user_rdd8 = user_rdd7.map(f => (f._1._1, (f._1._1, f._1._2, f._2))).join(user_rdd6.map(f => (f._1._1, f._2)))
    val user_rdd9 = user_rdd8.map(f => (f._2._1._2, (f._2._1._1, f._2._1._2, f._2._1._3, f._2._2)))
    // 物品2：（（物品1：物品2：同现频次：物品1频次），物品2频次）
    val user_rdd10 = user_rdd9.join(user_rdd6.map(f => (f._1._1, f._2)))
    // 物品1：物品2：同现频次：物品1频次：物品2频次
    val user_rdd11 = user_rdd10.map(f => (f._2._1._1, f._2._1._2, f._2._1._3, f._2._1._4, f._2._2))
    // 物品1：(物品2：相似度)
    val user_rdd12 = user_rdd11
      .map(f => {
        val re = f._3 / sqrt(f._4 * f._5)
        if (re.isNaN) {
          val b1 = BigInt(f._4)
          val b2 = BigInt(f._5)
          (f._1, (f._2, (f._3 / sqrt((b1 * b2).toDouble))))
        } else {
          (f._1, (f._2, (f._3 / sqrt(f._4 * f._5))))
        }
      })
    // 选出与物品1相似度排前filterNum位的物品2，以减少数据量
    val user_rdd13 = user_rdd12.map(f => {
      (f._1, f._2 :: Nil)
    }).reduceByKey(_ ::: _).map(f => {
      val i2 = f._2.toBuffer
      val i2_2 = i2.sortBy(_._2)(Ordering[Double].reverse) // 逆序
      if (i2_2.length > filterNum) i2_2.remove(filterNum, i2_2.length - filterNum)
      (f._1, i2_2.toIterator)
    })
    // 物品2：物品1：相似度
    val user_rdd14 = user_rdd13.flatMap(f => {
      val sims = f._2
      for (s <- sims) yield (f._1, s._1, s._2)
    })

    user_rdd14
  }

  /**
    * 给某用户推荐物品
    *
    * @param items_similar
    * @param user_perf
    * @param r_number
    * @return
    */
  def recommend(items_similar: RDD[(String, String, Double)],
                user_perf: RDD[(String, String, Double)],
                r_number: Int): RDD[(String, String, Double)] = {
    val sim = items_similar.map(f => (f._1 + "-" + RandomUtil.getRandom(1000), (f._2, f._3))).partitionBy(new NumberPartitioner(1000))
      .map(f => {
        val keyWithSuffix = f._1
        val key = keyWithSuffix.substring(0, keyWithSuffix.lastIndexOf("-"))
        (key, f._2)
      })
    val user = user_perf.map(f => (f._2 + "-" + RandomUtil.getRandom(1000), (f._1, f._3))).partitionBy(new NumberPartitioner(1000))
      .map(f => {
        val keyWithSuffix = f._1
        val key = keyWithSuffix.substring(0, keyWithSuffix.lastIndexOf("-"))
        (key, f._2)
      })
    // 物品1：（（物品2：物品1、2相似度）：（用户：用户对物品1评分））
    val rdd_R2 = user.join(sim)

    // 排除已经评分元素
    // 计算用户对物品的预测分  （用户：物品2）：用户对物品1的评分*物品1、2的相似度
    val user_rated = user.map(f => ((f._2._1, f._1), f._2._2))
    val rdd_R3 = rdd_R2.map(f => ((f._2._1._1, f._2._2._1), (f._2._1._2, f._2._2._2))).subtractByKey(user_rated)
      .map(f => (f._1, f._2._1 * f._2._2))

    // 元素累加求和  用户：（物品：预测分）
    val rdd_R4 = rdd_R3.reduceByKey((x, y) => x + y).map(f => (f._1._1, (f._1._2, f._2)))

    // 用户对结果排序，过滤
//    val rdd_R5 = rdd_R4.groupByKey()
    val rdd_R5 = rdd_R4.map(f => {
      (f._1, f._2 :: Nil)
    }).reduceByKey(_ ::: _)
    val rdd_R6 = rdd_R5.map(f => {
      val i2 = f._2.toBuffer
      val i2_2 = i2.sortBy(_._2)(Ordering[Double].reverse) // 逆序
      if (i2_2.length > r_number) i2_2.remove(0, (i2_2.length - r_number))
      (f._1, i2_2.toIterator)
    })

    val rdd_R7 = rdd_R6.flatMap(f => {
      val id2 = f._2
      for (w <- id2) yield (f._1, w._1, w._2)
    })

    rdd_R7
  }

  /**
    * 从数据库获取APP详细信息形成json数据
    * @param spark
    * @param imeis
    * @param ranks
    * @return
    */
  def genDetail(spark: SparkSession,
                imeis: RDD[(String, String)],
                ranks: RDD[(String, String, Double)]):RDD[(String, String)] = {
    // APP列表
    val appList = DbUtil.getAppList(spark)
    val appRdd = appList.rdd.map(a => {
      val pkg = a.getString(0)
      val downloadurl = a.getString(1)
      val appname = a.getString(2)
      val icon = a.getString(3)
      val csize = a.getDouble(4)
      val cversion = a.getInt(5)

      (pkg, (pkg, appname, downloadurl, icon, csize, cversion))
    })

    // 推荐结果
    val recommend = ranks.map(f => (f._1, (f._2, f._3))).join(imeis).map(f => {
      val imeiid = f._2._2
      val pkg = f._2._1._1
      val prediction = f._2._1._2
      (pkg, (imeiid, prediction))
    }).join(appRdd)
      .map(f => {
        val appData = f._2._2
        val imeiid = f._2._1._1
        val prediction = f._2._1._2
        (imeiid, (prediction, appData) :: Nil)
      }).reduceByKey(_ ::: _)
      .map(f => {
        val imeiid = f._1
        val appDatas = f._2.sortBy(_._1)
        val arr = new JSONArray()
        // 反向遍历
        for (i <- 0 until appDatas.length reverse) {
          val obj = new JSONObject()

          val pkgname = appDatas(i)._2._1
          val appname = appDatas(i)._2._2
          val durl = appDatas(i)._2._3
          val icon = appDatas(i)._2._4
          val csize = appDatas(i)._2._5
          val cversion = appDatas(i)._2._6
          val prediction = appDatas(i)._1
          obj.put("appname", appname)
          obj.put("pkgname", pkgname)
          obj.put("durl", durl)
          obj.put("icon", icon)
          obj.put("csize", csize)
          obj.put("cversion", cversion)
          obj.put("prediction", prediction)
          arr.add(obj)
        }
        (imeiid, arr.toJSONString())
      })
    recommend
  }

  /**
    * 从数据库获取APP详细信息形成json数据
    * @param spark
    * @param imeis
    * @param ranks
    * @return
    */
  def genDetail(spark: SparkSession,
                imeis: RDD[(String, String)],
                ranks: RDD[(String, String, Double)],
                broadcast: Broadcast[Array[String]]):RDD[(String, String)] = {
    // APP列表
    val appList = DbUtil.getAppList(spark)
    val appRdd = appList.rdd.map(a => {
      val pkg = a.getString(0)
      val downloadurl = a.getString(1)
      val appname = a.getString(2)
      val icon = a.getString(3)
      val csize = a.getDouble(4)
      val cversion = a.getInt(5)
      val infourl = a.getString(6)

      (pkg, (pkg, appname, downloadurl, icon, csize, cversion, infourl))
    })
    // 推荐结果
    val recommend = ranks.map(f => (f._1, (f._2, f._3))).join(imeis).map(f => {
      val imeiid = f._2._2
      val pkg = f._2._1._1
      val prediction = f._2._1._2
      (pkg, (imeiid, prediction))
    }).join(appRdd)
      .map(f => {
        val appData = f._2._2
        val imeiid = f._2._1._1
        val prediction = f._2._1._2
        (imeiid, (prediction, appData) :: Nil)
      }).reduceByKey(_ ::: _)
      .map(f => {
        val broadcastApp = broadcast.value
        val imeiid = f._1
        val appDatas = f._2.sortBy(_._1)
        val arrRmd = new ArrayBuffer[(Double, (String, String, String, String, Double, Int, String))]()
        val arrOther = new ArrayBuffer[(Double, (String, String, String, String, Double, Int, String))]()
        val arrAll = new ArrayBuffer[(Double, (String, String, String, String, Double, Int, String))]()
        // 反向遍历
        for (i <- 0 until appDatas.length reverse) {
          val prediction = appDatas(i)._1
          val pkgname = appDatas(i)._2._1

          if(broadcastApp.contains(pkgname)) {
            arrRmd += ((prediction, appDatas(i)._2))
          } else {
            arrOther += ((prediction, appDatas(i)._2))
          }
        }
        arrAll ++= arrRmd
        arrAll ++= arrOther
        // 最后推荐9个
        if (arrAll.size > 9) arrAll.remove(9, arrAll.size - 9)
        (imeiid, arrAll.toList)
      }).map(f => {
      val imeiid = f._1
      val appDatas = f._2
      val arr = new JSONArray()
      // 反向遍历
      for (i <- 0 until appDatas.length) {
        val obj = new JSONObject()

        val pkgname = appDatas(i)._2._1
        val appname = appDatas(i)._2._2
        val durl = appDatas(i)._2._3
        val icon = appDatas(i)._2._4
        val csize = appDatas(i)._2._5
        val cversion = appDatas(i)._2._6
        val infourl = appDatas(i)._2._7
        val prediction = appDatas(i)._1
        obj.put("appname", appname)
        obj.put("pkgname", pkgname)
        obj.put("durl", durl)
        obj.put("icon", icon)
        obj.put("csize", csize)
        obj.put("cversion", cversion)
        obj.put("infourl", infourl)
        obj.put("prediction", prediction)
        arr.add(obj)
      }
      (imeiid, arr.toJSONString())
    })

    recommend
  }
}
