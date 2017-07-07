package com.lanny.analysistest

import com.lanny.analysistest.util.{DbUtil, HdfsUtil, ImeiUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql._


/**
  * Created by victor on 2016/10/26.
  */
object AppRecommender {

  val RECOMMEND_APP = "t_recommend_app"
  val RESULT_TABLENAME = "t_recommend_indiv"
  val HDFS_RECOMMEND_APP = "/changmi/recommend/result/app/"


  // 20170414
  def main(args: Array[String]) {
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
    val sparkConf = new SparkConf()
        .setAppName("APP Recommendation_" + args(0))
        .set("spark.rpc.netty.dispatcher.numThreads", "2") //预防RpcTimeoutException
    val spark = SparkSession
        .builder()
        .config(sparkConf)
        .enableHiveSupport()
        .getOrCreate()

    import spark.sql

    sql("use arrival")

    // 原始数据后面会多次使用，持久化
    val rawUserAppData = sql("select imei as user,pkg as app,rating from mid_recommend_rating where year=" + year + " and month=" + month + " and day=" + day)

    // 通过JDBC查询所有imei匹配
    val imeisRDD = ImeiUtil.getAllImeis_V2(spark, year, month, day)

    // 保留所有imei
    val imeisFormat = imeisRDD.map(f => {
      val imeiid = f._2
      val imei = f._1
      (imeiid, imei :: Nil)
    }).reduceByKey((a, b) => {
      a ::: b
    })

    //实例化对象
    val recommender = new AppRecommend(spark)
    // 计算推荐结果
    val result_all = recommender.recommend(rawUserAppData, imeisRDD, null)

    val resultDir = HDFS_RECOMMEND_APP + year + "/" + month + "/" + day
    // 先删除HDFS的目录
    HdfsUtil.deleteDir(spark.sparkContext, resultDir)
    result_all.map(r => r._1 + "|" + r._2).repartition(1).saveAsTextFile(resultDir)

    //停止spark
    spark.stop()

    val rt = Runtime.getRuntime()
    val command = "nohup /data/install/spark-2.0.0-bin-hadoop2.7/bin/spark-submit --master spark://hxf:7077  --executor-memory 20G --total-executor-cores 8  --conf spark.sql.shuffle.partitions=1000 --supervise --class com.lanny.analysistest.ResultToDB --jars /home/hadoop/jar/fastjson-1.2.1.jar /home/hadoop/jar/recommendation.jar  " + args(0) + " app > /home/hadoop/logs/AppResultToDB.log &"

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
}
