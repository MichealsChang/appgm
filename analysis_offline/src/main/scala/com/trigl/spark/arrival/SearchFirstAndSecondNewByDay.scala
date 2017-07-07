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
 * 统计指定时间区域内首次到达和二次有效到达 针对新版本 逐天进行
 */
object SearchFirstAndSecondNewByDay {

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
    System.setProperty("spark.sql.crossJoin.enabled", "true") // 允许join笛卡尔积
    val sparkConf = new SparkConf().setAppName("SearchFirstAndSecondNewByDay_" + args(0))
    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
    println("Start " + "SearchFirstAndSecondNewByDay_" + args(0))

    import spark.implicits._
    import spark.sql
    sql("use arrival")

    // imei库
    val imeisRDD = ImeiUtil.getAllImeis_V2(spark, year, month, day).toDF("imei", "imeiid")
    imeisRDD.createOrReplaceTempView("imeiDic")
    // 保留唯一imei的imei库
    val uniqImei = sql("select min(imei) imei, imeiid from imeiDic group by imeiid")
    uniqImei.createOrReplaceTempView("uniqImei")

    // 以前所有首次到达数据
    val firstOld = sql("select i.imeiid, f.arrtime, f.simstatus from dim_arr_first_new f " +
      "left join imeiDic i on f.imei = i.imei")

    // 每天到达数据
    val rawData = sql("select (case when (firstimei != ' ') then firstimei when (secondimei != ' ') then secondimei when (thirdimei != ' ') then thirdimei else null end) as imei, " +
      "accepttimestamp arrtime, simstatus from base_arrival_new " +
      "where year = " + year + " and month = " + month + " and day = " + day)
    // 创建临时表
    rawData.createOrReplaceTempView("rawData")
    // 规范化新到达数据
    val firstNew = sql("select i.imeiid, r.arrtime, r.simstatus from rawData r " +
      "left join imeiDic i on r.imei = i.imei")
    firstNew.createOrReplaceTempView("firstNew")

    val firstAll = firstNew.union(firstOld)
    firstAll.createOrReplaceTempView("firstAll")

    // 第一次到达数据
    val firstArr = sql("select a.imeiid, min(a.arrtime) arrtime, min(a.simstatus) simstatus " +
      "from firstAll a group by a.imeiid")
    firstArr.createOrReplaceTempView("firstArr")
    val firstArrImei = sql("select distinct ui.imei, a.arrtime, a.simstatus from firstArr a " +
      "left join uniqImei ui on a.imeiid = ui.imeiid")
    firstArrImei.repartition(1).createOrReplaceTempView("firstArrImei")
    // 第一次到达数据入Hive表
    sql("INSERT OVERWRITE TABLE dim_arr_first_new " +
      "PARTITION (year='" + year + "',month='" + month + "',day='" + day + "') " +
      "select fa.imei, fa.arrtime, fa.simstatus FROM firstArrImei fa")

    // 除去第一次到达的数据
    val otherArr = firstAll.except(firstArr)
    otherArr.createOrReplaceTempView("otherArr")

    // 上一次二次到达数据
    val secondOld = sql("select i.imeiid, s.arrtime, s.simstatus from dim_arr_second_new s " +
      "left join imeiDic i on s.imei = i.imei")
    // 所有二次数据
    val secondAll = secondOld.union(otherArr)
    secondAll.createOrReplaceTempView("secondAll")

    // 先找出一定有二次到达的数据（1，与首次到达相隔两小时,2，必须是插卡状态）
    val moreArr = sql("select sa.imeiid, sa.arrtime, sa.simstatus from secondAll sa " +
      "inner join firstArr fa on sa.imeiid = fa.imeiid " +
      "where sa.arrtime - fa.arrtime >= " + INTERVAL_BETWEEN_FIRST_AND_SECEOND + " and sa.simstatus = 1")
    moreArr.createOrReplaceTempView("moreArr")
    // 第二次到达数据
    val secondArr = sql("select min(ui.imei) imei, min(a.arrtime) arrtime, min(a.simstatus) simstatus from moreArr a " +
      "left join uniqImei ui on a.imeiid = ui.imeiid group by a.imeiid")
    secondArr.repartition(1).createOrReplaceTempView("secondArr")
    // 第二次到达数据入Hive表
    sql("INSERT OVERWRITE TABLE dim_arr_second_new " +
      "PARTITION (year='" + year + "',month='" + month + "',day='" + day + "') " +
      "select sa.imei, sa.arrtime, sa.simstatus FROM secondArr sa")

    spark.stop()

/*    //保存log
    saveLog(args(0) + "T" + args(0))

    val rt = Runtime.getRuntime()
    //val cmd = Array("/bin/sh", "-c", "nohup /data/install/spark/bin/spark-submit --master spark://fenxi-xlg:7077  --executor-memory 20G --total-executor-cores 10 --name Data-Analyze --supervise --class com.analysis.test.prd.channel.SearchFirstAndSecondFinalFinal_V2 --jars /data/install/hbase/lib/hbase-client-1.2.2.jar,/data/install/hbase/lib/hbase-server-1.2.2.jar,/data/install/hbase/lib/hbase-common-1.2.2.jar,/data/install/hbase/lib/hbase-protocol-1.2.2.jar,/data/install/hbase/lib/guava-12.0.1.jar,/data/install/hbase/lib/htrace-core-3.1.0-incubating.jar,/data/install/hbase/lib/metrics-core-2.2.0.jar,/home/hadoop/jars/mysql-connector-java-5.1.25.jar,/home/hadoop/jars/fastjson-1.2.1.jar,/home/hadoop/jars/jedis-2.7.2.jar,/home/hadoop/jars/commons-pool2-2.3.jar,/home/hadoop/jars/pinyin4j-2.5.0.jar /home/hadoop/analysis-0.0.1-SNAPSHOT.jar "+year+"-"+month+"-"+day+" 00:00:00 "+year+"-"+month+"-"+day+" 23:59:59 120 > /home/hadoop/spark_hbase_hdfs_v2.out &");
    val cmd = Array("/bin/sh", "-c", "nohup /data/install/spark/bin/spark-submit --master spark://fenxi-xlg:7077  --executor-memory 20G --total-executor-cores 10 --name Data-Analyze --supervise --class com.analysis.test.prd.channel.SearchFirstAndSecondFinalBeta_V2 --jars /home/hadoop/jars/mysql-connector-java-5.1.25.jar /home/hadoop/analysis-0.0.1-SNAPSHOT.jar " + year + month + day + " > /home/hadoop/logs/spark_hbase_hdfs_v2.out &");

    try {
      val proc = rt.exec(cmd)
      // 获取进程的标准输入流
      val exitVal = proc.waitFor()
      proc.destroy()
      println("跑新版本到达数据任务提交")
    } catch {
      case e: Exception =>
        println(args(0) + "跑新版本到达数据失败：" + e.getMessage)
    }*/
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