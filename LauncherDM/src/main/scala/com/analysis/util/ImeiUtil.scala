package com.analysis.util

import java.sql.ResultSet
import java.sql.DriverManager

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import java.sql.Connection
import java.sql.Statement

import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import java.util.Calendar
import java.text.SimpleDateFormat

object ImeiUtil {
  val NULL_IMEI = "0000000000000000"
  val NULL_IMEI_15 = "000000000000000"
  val NULL_IMEI_14 = "00000000000000"
  val IMEI_ALL_TABLE = "t_imei_all"
  val IMEI_ARR_TABLE = "t_imei_arr"
  val IMEI_CPZ_TABLE = "t_imei_cpz"

  def parseLength(str: String): String = {
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

  def toImei(param: String): String = {
    var imei = ""
    try {
      if (param.startsWith("99") || param.startsWith("a") || param.startsWith("A"))
        imei = param.substring(0, 14)
      else imei = param.substring(0, 15)
      imei
    } catch {
      case e: Exception =>
        println("error imei:" + param)
        ""
    }

  }


  def getAllImeis(spark: SparkSession, year: String, month: String, day: String): DataFrame = {
    val maxId = allImeiMaxId(year, month, day)

    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val imeis = spark.read.format("jdbc").options(
      Map("url" -> DbUtil.IMEI_DB_URL,
        //"dbtable"-> "(SELECT imei,imeiid,num FROM t_imei) a",
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
    /*    val imeisRDD = imeis.rdd.map(t => {
          //t(0)  id
          val imei = t(1).toString
          val imeiid = t(2).toString
          val num = t(3).toString.toInt

          (ImeiUtil.parseLength(imei), (imei, imeiid, num))
        })
        imeisRDD*/
  }

  def getAllImeis(spark: SparkSession, year: String, month: String): DataFrame = {
    val maxId = allImeiMaxId(year, month)

    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val imeis = spark.read.format("jdbc").options(
      Map("url" -> DbUtil.IMEI_DB_URL,
        //"dbtable"-> "(SELECT imei,imeiid,num FROM t_imei) a",
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
/*    val imeisRDD = imeis.rdd.map(t => {
      //t(0)  id
      val imei = t(1).toString
      val imeiid = t(2).toString
      val num = t(3).toString.toInt

      (ImeiUtil.parseLength(imei), (imei, imeiid, num))
    })
    imeisRDD*/
  }

  def imeiMaxId(tableName: String): Long = {
    //检查imei库数据是否准备好
    //Class.forName("com.mysql.jdbc.Driver"); // "oracle.jdbc.driver.OracleDriver"

    var con: Connection = null
    var stmt: Statement = null
    var rsCheck: ResultSet = null
    var maxId: Long = 0
    try {
      con = DriverManager.getConnection(DbUtil.IMEI_DB_URL, DbUtil.IMEI_DB_USERNAME, DbUtil.IMEI_DB_PASSWORD);
      stmt = con.createStatement()
      rsCheck = stmt.executeQuery("select max(id) from " + tableName);
      while (rsCheck.next()) {
        maxId = rsCheck.getLong(1)

      }
    } catch {
      case e: Exception =>
        System.out.println("查询imei最大id失败");
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

  def allImeiMaxId(year: String, month: String, day: String): Long = {
    //检查imei库数据是否准备好
    //Class.forName("com.mysql.jdbc.Driver"); // "oracle.jdbc.driver.OracleDriver"

    var con: Connection = null
    var stmt: Statement = null
    var rs: ResultSet = null
    var rsCheck: ResultSet = null
    var maxId: Long = 0
    try {
      con = DriverManager.getConnection(DbUtil.IMEI_DB_URL, DbUtil.IMEI_DB_USERNAME, DbUtil.IMEI_DB_PASSWORD);
      stmt = con.createStatement()
      rs = stmt.executeQuery("select * from t_imei_log where timeflag = '" + year + month + day + "' and type = 3")
      if (!rs.next()) {
        System.err.println("IMEI库数据未准备好")
        System.exit(1)
      }
      rsCheck = stmt.executeQuery("select max(id) from " + IMEI_ALL_TABLE)
      while (rsCheck.next()) {
        maxId = rsCheck.getLong(1);

      }
    } catch {
      case e: Exception =>
        System.out.println("查询imei最大id失败")
    } finally {
      if (rs != null)
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

  def allImeiMaxId(year: String, month: String): Long = {
    //检查imei库数据是否准备好
    //Class.forName("com.mysql.jdbc.Driver"); // "oracle.jdbc.driver.OracleDriver"

    var con: Connection = null
    var stmt: Statement = null
    var rs: ResultSet = null
    var rsCheck: ResultSet = null
    var maxId: Long = 0
    try {
      val sdf = new SimpleDateFormat("yyyy-MM")

      val calendar = Calendar.getInstance()
      calendar.setTime(sdf.parse(year + "-" + month))
      val maxDays = calendar.getActualMaximum(Calendar.DAY_OF_MONTH)
      con = DriverManager.getConnection(DbUtil.IMEI_DB_URL, DbUtil.IMEI_DB_USERNAME, DbUtil.IMEI_DB_PASSWORD)
      stmt = con.createStatement()
      rs = stmt.executeQuery("select * from t_imei_log where timeflag like '" + year + month + "%' and type = 3")
      var days = 0
      while (rs.next()) {
        days = days + 1
      }
      if (days != maxDays) {
        System.err.println("IMEI库数据未准备好")
        System.exit(1)
      }
      rsCheck = stmt.executeQuery("select max(id) from " + IMEI_ALL_TABLE)
      while (rsCheck.next()) {
        maxId = rsCheck.getLong(1)

      }
    } catch {
      case e: Exception =>
        System.out.println("查询imei最大id失败")
    } finally {
      if (rs != null)
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