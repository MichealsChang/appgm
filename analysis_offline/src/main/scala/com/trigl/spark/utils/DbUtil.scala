package com.trigl.spark.utils

import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.Connection
import java.sql.Statement

import org.apache.commons.lang.StringUtils

import scala.collection.mutable.ArrayBuffer

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
 
  
  
  //获取uid和companyid对应map
  def getUserCompanyRela():(Map[String,String],Map[String,String])={
     //进行数据匹配客户
      var dbconn: Connection = null  
      var stmt:Statement = null
      var rs:ResultSet = null
      var oldMap = Map[String,String]()//2016-11-7号之前的uid，company对应关系(给老的上报接口使用)
      var newMap = Map[String,String]()
      try{
        dbconn = DriverManager.getConnection(DbUtil.ADMIN_DB_URL, DbUtil.ADMIN_DB_USERNAME, DbUtil.ADMIN_DB_PASSWORD)
      
        stmt = dbconn.createStatement();
       
        rs = stmt.executeQuery("select olduid,companyid from t_clientuser_chart where newuid is not null union select clientuserid,companyid from t_clientuser where clientuserid not in (select olduid from t_clientuser_chart where newuid is not null)");
        while (rs.next()) {
          val uid = rs.getInt(1).toString()
          val companyid = rs.getInt(2).toString()
          oldMap +=(uid->companyid)
        }
        
        rs = stmt.executeQuery("select clientuserid,companyid from t_clientuser union select olduid,companyid from t_clientuser_chart where olduid not in (select clientuserid from t_clientuser)");
        while (rs.next()) {
          val uid = rs.getInt(1).toString()
          val companyid = rs.getInt(2).toString()
          newMap +=(uid->companyid)
        }
      }catch{
        case e:Exception=>
          System.out.println("统计数据失败:"+e.printStackTrace());
      }finally{
        if(rs!=null)
          rs.close()
        if(stmt!=null)
          stmt.close()
        if(dbconn!=null)
          dbconn.close()
      }
      (oldMap,newMap)
      
  }

}