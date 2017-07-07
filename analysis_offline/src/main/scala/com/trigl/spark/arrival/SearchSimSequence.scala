package com.trigl.spark.arrival

import com.trigl.spark.utils.ImeiUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * 匹配对应的sim卡序列号
  * created by Trigl at 2017-06-20 14:15
  */
object SearchSimSequence {

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    System.setProperty("spark.network.timeout", "300s")
    System.setProperty("spark.sql.crossJoin.enabled", "true") // 允许join笛卡尔积
    val sparkConf = new SparkConf().setAppName("SearchSimSequence")
    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
    println("Start " + "SearchSimSequence")

    import spark.implicits._
    import spark.sql
    sql("use arrival")

    // imei库
    val imeisRDD = ImeiUtil.getAllImeis_V2(spark, null, null, null).toDF("imei", "imeiid")
    imeisRDD.createOrReplaceTempView("imeiDic")
    // 保留唯一imei的imei库
    val uniqImei = sql("select min(imei) imei, imeiid from imeiDic group by imeiid")
    uniqImei.createOrReplaceTempView("uniqImei")

    // 广告主原始要查询报表
    val adData = spark.sparkContext
      .textFile("/test/qinglidashi_20170501T20170531_imei_second_and_third_and_fourth_and_fifth_arr.csv")
      .map(f => {
        val arr = f.split(",")
        (arr(10).trim, f)
      }).toDF("imei", "rawdata")
    adData.createOrReplaceTempView("addata")

    // 规范后的数据
    val adDF = sql("select i.imeiid, ad.rawdata from addata ad left join imeiDic i on ad.imei = i.imei")
    adDF.createOrReplaceTempView("adDF")

    // 原始到达日志
    val arriveLog = sql("select firstimei as imei, simsequence from base_arrival_new where year='2017' and month='05' " +
      "union select firstimei as imei, simsequence from base_arrival_old where year='2017' and month='05'")
    arriveLog.createOrReplaceTempView("arriveLog")

    // 规范后的到达数据
    val arriveDF = sql("select i.imeiid, max(arr.simsequence) as simsequence from arriveLog arr left join imeiDic i on arr.imei = i.imei group by i.imeiid")
    arriveDF.createOrReplaceTempView("arriveDF")

    val resultDF = sql("select ad.rawdata, arr.simsequence from adDF ad left join arriveDF arr on ad.imeiid = arr.imeiid")
    val resultRDD = resultDF.rdd.map(r => r.getString(0) + "," + r.getString(1))
    resultRDD.repartition(1).saveAsTextFile("/test/simSearch")

    spark.stop()
  }
}
