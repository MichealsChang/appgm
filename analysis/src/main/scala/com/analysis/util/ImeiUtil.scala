package com.analysis.util

import java.sql.{Connection, DriverManager, ResultSet, Statement}

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

object ImeiUtil {
  val NULL_IMEI = "0000000000000000"
  val NULL_IMEI_15 = "000000000000000"
  val NULL_IMEI_14 = "00000000000000"
  val IMEI_ALL_TABLE = "t_imei_all"
  val IMEI_ARR_TABLE = "t_imei_arr"
  val IMEI_CPZ_TABLE = "t_imei_cpz"

  def parseLength(str: String):String = {
    if (str != null) {
      val sb = new StringBuilder(str)
      while (sb.length < 16) {
        sb.append("0")
      }
      sb.toString().substring(0, 16)
    } else {
      NULL_IMEI
    }
  }

  def toImei(param: String):String = {
    var imei = ""
    try{
      if (param.startsWith("99") || param.startsWith("a") || param.startsWith("A"))
        imei = param.substring(0, 14)
      else imei = param.substring(0, 15)
      imei
    }catch{
      case e:Exception=>
        println("error imei:"+param)
        e.printStackTrace()
        ""
    }
    
  }
  
  
  
  def getAllImeis(ss:SparkSession, year:String, month:String, day:String): DataFrame={
    val maxId = allImeiMaxId(year,month,day)
    
    val imeis = ss.read.format("jdbc").options(
      Map("url" -> DbUtil.IMEI_DB_URL,
//        "dbtable"-> "(SELECT id,imei,imeiid FROM t_imei_all) a",
        "dbtable" -> IMEI_ALL_TABLE,
        "user" -> DbUtil.IMEI_DB_USERNAME,
        "password" -> DbUtil.IMEI_DB_PASSWORD,
        "driver" -> "com.mysql.jdbc.Driver", //"com.mysql.jdbc.Driver","oracle.jdbc.driver.OracleDriver"
        //"fetchSize" -> "1000",
        "partitionColumn" -> "id",
        "lowerBound" -> "1",
        "upperBound" -> maxId.toString(),
        "numPartitions" -> "20")).load()

    imeis
  }


 def imeiMaxId(tableName:String): Long = {
    //检查imei库数据是否准备好
    //Class.forName("com.mysql.jdbc.Driver"); // "oracle.jdbc.driver.OracleDriver"

    var con: Connection = null
    var stmt: Statement = null
    var rsCheck: ResultSet = null
    var maxId: Long = 0
    try {
      con = DriverManager.getConnection(DbUtil.IMEI_DB_URL, DbUtil.IMEI_DB_USERNAME, DbUtil.IMEI_DB_PASSWORD);
      stmt = con.createStatement();
      rsCheck = stmt.executeQuery("select max(id) from " + tableName)
      while (rsCheck.next()) {
        maxId = rsCheck.getLong(1);

      }
    } catch {
      case e: Exception =>
        println("查询imei最大id失败")
        e.printStackTrace()
    } finally {
      if (rsCheck != null)
        rsCheck.close()
      if (stmt != null)
        stmt.close()
      if (con != null)
        con.close()
    }
    if (maxId == 0) {
      System.err.println("imei id 异常")
      System.exit(1)
    }
    maxId
  }
 
 def allImeiMaxId(year:String,month:String,day:String): Long = {
    //检查imei库数据是否准备好
    //Class.forName("com.mysql.jdbc.Driver"); // "oracle.jdbc.driver.OracleDriver"

    var con: Connection = null
    var stmt: Statement = null
    var rs:ResultSet = null
    var rsCheck: ResultSet = null
    var maxId: Long = 0
   var query = ""
    try {
      con = DriverManager.getConnection(DbUtil.IMEI_DB_URL, DbUtil.IMEI_DB_USERNAME, DbUtil.IMEI_DB_PASSWORD)
      stmt = con.createStatement()
      if (StringUtils.isNotBlank(year) && StringUtils.isNotBlank(month) && StringUtils.isNotBlank(day)) {
        query = "select * from t_imei_log where timeflag = '"+year+month+day+"' and type = 3"
      } else if(StringUtils.isNotBlank(year) && StringUtils.isNotBlank(month) && StringUtils.isBlank(day)) {
        query = "select * from t_imei_log where timeflag like '"+year+month+"%' and type = 3"
      } else if(StringUtils.isNotBlank(year) && StringUtils.isBlank(month) && StringUtils.isBlank(day)) {
        query = "select * from t_imei_log where timeflag like '"+year+"%' and type = 3"
      } else if(StringUtils.isBlank(year) && StringUtils.isBlank(month) && StringUtils.isBlank(day)) {
        query = "select * from t_imei_log where type = 3"
      }
      rs = stmt.executeQuery(query)
      if(!rs.next()){
        System.err.println("IMEI库数据未准备好")
        System.exit(1)
      }
      rsCheck = stmt.executeQuery("select max(id) from " + IMEI_ALL_TABLE)
      while (rsCheck.next()) {
        maxId = rsCheck.getLong(1)

      }
    } catch {
      case e: Exception =>
        println("查询imei最大id失败")
        e.printStackTrace()
    } finally {
      if(rs!=null)
        rs.close()
      if (rsCheck != null)
        rsCheck.close()
      if (stmt != null)
        stmt.close()
      if (con != null)
        con.close()
    }
    if (maxId == 0) {
      System.err.println("imei id 异常")
      System.exit(1)
    }
    maxId
  }

}