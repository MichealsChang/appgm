package com.trigl.spark.arrival

import java.sql.{Connection, DriverManager, PreparedStatement, Timestamp}
import java.util.Date

import com.trigl.spark.utils.{DbUtil, ImeiUtil}
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * *
 * 统计指定时间区域内首次到达和二次有效到达 针对新版本 第一次初始化
 */
object SearchFirstAndSecondNewInit {

  val sdf = FastDateFormat.getInstance("yyyyMMddHHmmssSSS")
  val INTERVAL_BETWEEN_FIRST_AND_SECEOND = 2 * 60 * 60 * 1000 //2 hour
  val FIRSTANDSECOND_LOG_TABLE = "r_firstandsecond_log"

  def main(args: Array[String]) {
    // 参数格式 20160912
    if (args == null || args.length < 1) {
      System.err.println("参数异常")
      System.exit(1)
    }
    val year = args(0).substring(0, 4)
    val month = args(0).substring(4, 6)
    val day = args(0).substring(6, 8)

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    System.setProperty("spark.network.timeout", "300s")
    val sparkConf = new SparkConf().setAppName("SearchFirstAndSecondNewInit_" + args(0))
    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
    println("Start " + "SearchFirstAndSecondNewInit_" + args(0))

    import spark.implicits._
    import spark.sql
    sql("use arrival")

    // 所有到达数据
    val rawData = sql("select (case when (firstimei != ' ') then firstimei when (secondimei != ' ') then secondimei when (thirdimei != ' ') then thirdimei else null end) as imei, " +
      "accepttimestamp arrtime, simstatus from base_arrival_new")
//      "where year = " + year + " and month = " + month + " and day = " + day)
    // 创建临时表
    rawData.createOrReplaceTempView("rawData")

    // imei库
    val imeisRDD = ImeiUtil.getAllImeis_V2(spark, year, month, day).toDF("imei", "imeiid")
    imeisRDD.createOrReplaceTempView("imeiDic")
    // 保留唯一imei的imei库
    val uniqImei = sql("select min(imei) imei, imeiid from imeiDic group by imeiid")
    uniqImei.createOrReplaceTempView("uniqImei")

    // 规范化的所有到达数据
    val allArr = sql("select i.imeiid, r.arrtime, r.simstatus from rawData r " +
      "left join imeiDic i on r.imei = i.imei")
    allArr.createOrReplaceTempView("allArr")

    // 第一次到达数据
    val firstArr = sql("select a.imeiid, min(a.arrtime) arrtime, min(a.simstatus) simstatus " +
      "from allArr a group by a.imeiid")
    firstArr.createOrReplaceTempView("firstArr")
    val firstArrImei = sql("select distinct ui.imei, a.arrtime, a.simstatus from firstArr a " +
      "left join uniqImei ui on a.imeiid = ui.imeiid")
    firstArrImei.repartition(1).createOrReplaceTempView("firstArrImei")
    // 第一次到达数据入Hive表
    sql("INSERT OVERWRITE TABLE dim_arr_first " +
      "PARTITION (year='" + year + "',month='" + month + "',day='" + day + "') " +
      "select fa.imei, fa.arrtime, fa.simstatus FROM firstArrImei fa")

    // 除去第一次到达的数据
    val otherArr = allArr.except(firstArr)
    otherArr.createOrReplaceTempView("otherArr")

    // 先找出一定有二次到达的数据（1，与首次到达相隔两小时,2，必须是插卡状态）
    val moreArr = sql("select oa.imeiid, oa.arrtime, oa.simstatus from otherArr oa " +
      "inner join firstArr fa on oa.imeiid = fa.imeiid " +
      "where oa.arrtime - fa.arrtime >= " + INTERVAL_BETWEEN_FIRST_AND_SECEOND + " and oa.simstatus = 1")
    moreArr.createOrReplaceTempView("moreArr")
    // 第二次到达数据
    val secondArr = sql("select min(ui.imei) imei, min(a.arrtime) arrtime, min(a.simstatus) simstatus from moreArr a " +
      "left join uniqImei ui on a.imeiid = ui.imeiid group by a.imeiid")
    secondArr.repartition(1).createOrReplaceTempView("secondArr")
    // 第二次到达数据入Hive表
    sql("INSERT OVERWRITE TABLE dim_arr_second " +
      "PARTITION (year='" + year + "',month='" + month + "',day='" + day + "') " +
      "select sa.imei, sa.arrtime, sa.simstatus FROM secondArr sa")

    spark.stop()

  }

  def saveLog(daterange: String) = {
    var dbconn: Connection = null
    var ps: PreparedStatement = null
    val sql = "insert into " + FIRSTANDSECOND_LOG_TABLE + "(daterange,startdate,enddate,type,createtime) values (?,?,?,?,?)"
    try {
      dbconn = DriverManager.getConnection(DbUtil.IMEI_DB_URL, DbUtil.IMEI_DB_USERNAME, DbUtil.IMEI_DB_PASSWORD)
      ps = dbconn.prepareStatement(sql)
      val now = new Timestamp(new Date().getTime())
      val datarange = daterange.split("T")
      val start = datarange(0)
      val end = datarange(1)
      ps.setString(1, daterange)
      ps.setString(2, start)
      ps.setString(3, end)
      ps.setInt(4, 1)
      ps.setTimestamp(5, now)
      //ps.addBatch()
      ps.execute()

    } catch {
      case e: Exception =>
        println("Sql Exception:" + e.getMessage)

    } finally {
      if (ps != null)
        ps.close()

      if (dbconn != null)
        dbconn.close()
    }
  }
}