package com.lanny.analysistest.util

import java.sql.{Connection, DriverManager, Statement}

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

object DbUtil {
  //运营数据库
  val ADMIN_DB_URL = "jdbc:mysql://rm-bp1f33sz58h0a8e2to.mysql.rds.aliyuncs.com:3306/tpdb?useUnicode=true&characterEncoding=UTF8"
  val ADMIN_DB_USERNAME = "rds_admin"
  val ADMIN_DB_PASSWORD = "changmi890*()"
  //渠道
  val CHANNEL_DB_URL = "jdbc:mysql://rm-bp12vrh51o66pw91io.mysql.rds.aliyuncs.com:3306/channeldb?useUnicode=true&characterEncoding=UTF8"
  val CHANNEL_DB_USERNAME = "rds_channel"
  val CHANNEL_DB_PASSWORD = "changmi890*()"

  //广告主
  val ADV_DB_URL = "jdbc:mysql://rm-bp1fpe4uezwc66339o.mysql.rds.aliyuncs.com:3306/adverdb?useUnicode=true&characterEncoding=UTF8"
  val ADV_DB_USERNAME = "rds_ad"
  val ADV_DB_PASSWORD = "changmi890*()"
  //IMEI库
  val IMEI_DB_URL = "jdbc:mysql://10.0.0.150:3306/IMEI?useUnicode=true&characterEncoding=UTF8"
  val IMEI_DB_USERNAME = "root"
  val IMEI_DB_PASSWORD = "changmi890*()"

  //应用市场库
  val APPCENTER_DB_URL = "jdbc:mysql://10.0.0.50:3208/acmsdb?useUnicode=true&characterEncoding=UTF8"
  val APPCENTER_DB_USERNAME = "root"
  val APPCENTER_DB_PASSWORD = "changmi890*()"

  //推荐市场库
  val RECOMMEND_DB_URL = "jdbc:mysql://10.0.0.50:3208/acmsdb?useUnicode=true&characterEncoding=UTF8"
//    val RECOMMEND_DB_URL = "jdbc:mysql://10.0.0.80:3306/recommend?useUnicode=true&characterEncoding=UTF8"
  val RECOMMEND_DB_USERNAME = "root"
  val RECOMMEND_DB_PASSWORD = "changmi890*()"


  def cleanTable(url: String, username: String, password: String, tablename: String) = {
    var dbconn: Connection = null
    var stmt: Statement = null
    try {
      dbconn = DriverManager.getConnection(url, username, password)
      stmt = dbconn.createStatement()
      stmt.execute("truncate table " + tablename)
    } catch {
      case e: Exception =>
        println(">>>>>>>>>>>>清空表失败")
        e.printStackTrace()
    } finally {
      if (stmt != null)
        stmt.close()
      if (dbconn != null)
        dbconn.close()
    }

  }

  /**
    * 获取主推应用
    * @param spark
    * @param folder
    * @return
    */
  def getRecommendApp(spark: SparkSession, folder: String): DataFrame = {

    var sqlStr = "" // 查询语句
    if (StringUtils.isNotBlank(folder)) {
      // “201常用”对应：2实用工具、4聊天与社交、7效率办公、14新闻
      if ("201".equals(folder)) {
        sqlStr = "(SELECT a.packagename as app,a.downloadurl,a.appname,a.icon,a.csize, a.cversion" +
          " FROM t_recommend_app ra LEFT JOIN t_app a ON ra.appid = a.appid and a.sortid in (2,4,7,14) WHERE a.isdel = 0) b"
      }
      // “202娱乐”对应：5图书与阅读、11摄影摄像、15娱乐消遣
      if ("202".equals(folder)) {
        sqlStr = "(SELECT a.packagename as app,a.downloadurl,a.appname,a.icon,a.csize, a.cversion" +
          " FROM t_recommend_app ra LEFT JOIN t_app a ON ra.appid = a.appid and a.sortid in (5,11,15) WHERE a.isdel = 0) b"
      }
      // “203飙升”对应：3影音视听、8时尚与购物、13体育与运动
      if ("203".equals(folder)) {
        sqlStr = "(SELECT a.packagename as app,a.downloadurl,a.appname,a.icon,a.csize, a.cversion" +
          " FROM t_recommend_app ra LEFT JOIN t_app a ON ra.appid = a.appid and a.sortid in (3,8,13) WHERE a.isdel = 0) b"
      }
      // “204必备”对应：6学习与教育、9生活、10旅行与交通
      if ("204".equals(folder)) {
        sqlStr = "(SELECT a.packagename as app,a.downloadurl,a.appname,a.icon,a.csize, a.cversion" +
          " FROM t_recommend_app ra LEFT JOIN t_app a ON ra.appid = a.appid and a.sortid in (6,9,10) WHERE a.isdel = 0) b"
      }
      // “205游戏对战”对应：1游戏
      if ("205".equals(folder)) {
        sqlStr = "(SELECT a.packagename as app,a.downloadurl,a.appname,a.icon,a.csize, a.cversion" +
          " FROM t_recommend_app ra LEFT JOIN t_app a ON ra.appid = a.appid and (a.sortid=1 or a.sortid>16) WHERE a.isdel = 0) b"
      }
      // “206其他”对应：12医疗与健康、16理财
      if ("206".equals(folder)) {
        sqlStr = "(SELECT a.packagename as app,a.downloadurl,a.appname,a.icon,a.csize, a.cversion" +
          " FROM t_recommend_app ra LEFT JOIN t_app a ON ra.appid = a.appid and a.sortid in (12,16) WHERE a.isdel = 0) b"
      }

    } else {
      sqlStr = "(SELECT a.packagename as app,a.downloadurl,a.appname,a.icon,a.csize, a.cversion" +
        " FROM t_recommend_app ra LEFT JOIN t_app a ON ra.appid = a.appid WHERE a.isdel = 0) b"
    }

    spark.read.format("jdbc").options(
      Map("url" -> DbUtil.APPCENTER_DB_URL,
        "dbtable"-> sqlStr,
        //        "dbtable" -> RECOMMEND_APP,
        "user" -> DbUtil.APPCENTER_DB_USERNAME,
        "password" -> DbUtil.APPCENTER_DB_PASSWORD,
        "driver" -> "com.mysql.jdbc.Driver" //"com.mysql.jdbc.Driver","oracle.jdbc.driver.OracleDriver"
        //"fetchSize" -> "1000",
        //        "partitionColumn" -> "id",
        //        "numPartitions" -> "20"
      )).load()
  }

  /**
    * 获取主推应用
    * @param spark
    * @return
    */
  def getRecommendApp(spark: SparkSession): DataFrame = {

    // 查询语句
    val sqlStr = "(SELECT a.packagename as pkg FROM t_recommend_app ra LEFT JOIN t_app a ON ra.appid = a.appid WHERE a.isdel = 0) b"
    spark.read.format("jdbc").options(
      Map("url" -> DbUtil.APPCENTER_DB_URL,
        "dbtable"-> sqlStr,
        //        "dbtable" -> RECOMMEND_APP,
        "user" -> DbUtil.APPCENTER_DB_USERNAME,
        "password" -> DbUtil.APPCENTER_DB_PASSWORD,
        "driver" -> "com.mysql.jdbc.Driver" //"com.mysql.jdbc.Driver","oracle.jdbc.driver.OracleDriver"
        //"fetchSize" -> "1000",
        //        "partitionColumn" -> "id",
        //        "numPartitions" -> "20"
      )).load()
  }

  def getAppList(spark: SparkSession): DataFrame = {
    // 查询语句
    val sqlStr = "(SELECT packagename as app,downloadurl,appname,icon,csize, cversion, infourl FROM t_app WHERE isdel = 0) b"
    spark.read.format("jdbc").options(
      Map("url" -> DbUtil.APPCENTER_DB_URL,
        "dbtable"-> sqlStr,
        //        "dbtable" -> RECOMMEND_APP,
        "user" -> DbUtil.APPCENTER_DB_USERNAME,
        "password" -> DbUtil.APPCENTER_DB_PASSWORD,
        "driver" -> "com.mysql.jdbc.Driver" //"com.mysql.jdbc.Driver","oracle.jdbc.driver.OracleDriver"
        //"fetchSize" -> "1000",
        //        "partitionColumn" -> "id",
        //        "numPartitions" -> "20"
      )).load()
  }
}