package com.analysis.util

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
 
}