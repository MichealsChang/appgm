package com.trigl.spark.preinstall

import java.net.URLDecoder
import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.util.{ArrayList, Date}

import com.trigl.spark.entity.{CpzApp, CpzSource}
import com.trigl.spark.utils.{DbUtil, HBaseUtil, HdfsUtil}
import org.apache.commons.lang.StringUtils
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HBaseAdmin, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import scala.collection.mutable.ArrayBuffer

/**
  * 解析预装数据，新版本
  */
object ParseBrushFinalSync {

  var conf: Configuration = null
  var conf_app: Configuration = null
  val sdf = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  def main(args: Array[String]) {

    //参数格式 2016-09-12 00:00:00 2016-09-12 07:59:59
    if (args == null || args.length < 1) {
      System.err.println("参数异常")
      System.exit(1)
    }

    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    System.setProperty("spark.network.timeout", "300s")
    val sparkConf = new SparkConf().setAppName("ParseBrushFinalSync_" + args(0))

    val sc = new SparkContext(sparkConf)

    // HBase连接
    val conn = HBaseUtil.getConnection(null)
    // HBase创建表操作
    val admin = conn.getAdmin
    if (!admin.isTableAvailable(TableName.valueOf(HBaseUtil.TABLE_NAME_CPZ_SOURCE_V2))) {
      val tableDesc = new HTableDescriptor(
        TableName.valueOf(HBaseUtil.TABLE_NAME_CPZ_SOURCE_V2))
      val hcd = new HColumnDescriptor(HBaseUtil.COLUMN_FAMILY)
      tableDesc.addFamily(hcd)
      admin.createTable(tableDesc)
    }

    if (!admin.isTableAvailable(TableName.valueOf(HBaseUtil.TABLE_NAME_CPZ_APP_V2))) {
      val tableDesc = new HTableDescriptor(
        TableName.valueOf(HBaseUtil.TABLE_NAME_CPZ_APP_V2))
      val hcd = new HColumnDescriptor(HBaseUtil.COLUMN_FAMILY)
      tableDesc.addFamily(hcd)
      admin.createTable(tableDesc)
    }
    admin.close()
    conn.close()

    // 从DB中获取机型品牌信息
    val phoneMap = getAllPhoneModelAndBrand()
    // 设置广播变量
    val broadInstance = sc.broadcast(phoneMap)

    val parseRDD = sc.textFile(args(0)).flatMap(f => {
      try {
        // 上报的日志作了GBK编码，首先进行解码
        var x = URLDecoder.decode(f, "GBK")
        var map = Map[String, String]()
        // 确保关键信息存在
        if (x.contains("clienttype") && x.contains("imei")) {

          val ip = x.substring(0, x.indexOf("|"))
          x = x.substring(x.indexOf("|") + 1)

          // 13位时间戳,接收服务器的接收的时间
          var tss = x.substring(0, x.indexOf("|"))
          tss = tss.replaceAll("\\.", "")
          val ts = tss.toLong
          val posttime = sdf.format(new Date(ts))
          map += ("posttime" -> posttime)
          x = x.substring(x.indexOf("|") + 1)

          val arr = x.split("&")
          for (a <- arr) {
            val t = a.split("=")
            if (t.length == 1) // XX=&YY=c的情况，对XX= split之后长度为1
              map += (t(0) -> "")
            else
              map += (t(0) -> t(1))
          }

          val time = map.getOrElse("time", "")
          if (StringUtils.isNotBlank(time)) {
            //客户端时间不为空

            var cpzSourceList = List[CpzSource]()
            var imeiStr: String = null
            var imei2Str: String = null

            // 一次可能上传多部设备，因此是imei列表
            var param = map.get("imei")
            if (!param.isEmpty)
              imeiStr = param.get
            param = map.get("imei2")
            if (!param.isEmpty)
              imei2Str = param.get
            var imei2List: Array[String] = null
            if (StringUtils.isNotBlank(imei2Str)) {
              imei2List = imei2Str.split(",")
            }
            if (StringUtils.isNotBlank(imeiStr)) {
              val imeiList = imeiStr.split(",")
              for ((v, i) <- imeiList.zipWithIndex) {
                var imei2: String = null
                if (imei2List != null && imei2List.length > i)
                  imei2 = imei2List(i)
                val tcpzsource = ParseBrush.toCpzSource((v, imei2), map)
                cpzSourceList = cpzSourceList.:+(tcpzsource)
              }
            }
            cpzSourceList
          } else
            null :: Nil

        } else {
          null :: Nil
        }
      } catch {
        case e: Exception =>
          null :: Nil
      }
    }).filter(_ != null)
      .map(x => {
        ((x.customid, x.cdate, x.ime1, x.ime2), x)
      }).partitionBy(new HashPartitioner(20))
      .reduceByKey((a, b) => {
        // 重复的留较早上报的，后续还有去重
        if (a.postdate < b.postdate)
          a
        else
          b
      })
      .map(f => f._2)
      .map(x => {
        try {
          // 初始化phoneid
          if (StringUtils.isBlank(x.phonemodel) || "0".equals(x.phonemodel)) {
            val phoneMap = broadInstance.value
            var xmodel = x.phonemodelname
            if (StringUtils.isNotBlank(xmodel))
              xmodel = xmodel.toUpperCase()
            var xbrand = x.brand
            if (StringUtils.isNotBlank(xbrand))
              xbrand = xbrand.toUpperCase()
            x.phonemodel = phoneMap.getOrElse((xmodel, xbrand), "0")
          }

          val packageList = x.packagelist
          var appList = List[CpzApp]()
          if (StringUtils.isNotBlank(packageList)) {
            val arr = packageList.split(";")
            var systemappcount = 0
            var userappcount = 0
            for (a <- arr) {
              if (StringUtils.isNotBlank(a)) {
                val appdetail = a.split(",")
                if ("0".equalsIgnoreCase(appdetail(3))) {
                  userappcount = userappcount + 1
                } else if ("1".equalsIgnoreCase(appdetail(3))) {
                  systemappcount = systemappcount + 1;
                }
                val appid = "0"
                val appdetailid: String = appdetail(0)
                val imid: String = null
                val state: String = null
                val adddate: String = sdf.format(new Date())
                val cpzasourceid: String = x.cpzsourceid
                var areatype: String = null
                if ("1".equalsIgnoreCase(appdetail(3))) {
                  areatype = "0"
                } else
                  areatype = "1"

                //重要且安装成功
                var impandsuc: String = null
                if (appdetail.length >= 5)
                  impandsuc = appdetail(4)
                val cdate: String = x.cdate
                val ime1: String = x.ime1
                val ime2: String = x.ime2
                val pkg: String = appdetail(1)
                val app = new CpzApp(appid, appdetailid, imid, state, adddate, cpzasourceid, areatype, cdate, ime1, ime2)

                app.pkg = pkg
                app.impandsuc = impandsuc
                appList = appList.:+(app)
              }
            }
            x.systemappcount = systemappcount.toString
            x.userappcount = userappcount.toString
            x.appList = appList
          }
          x
        } catch {
          case e: Exception =>
            null
        }
      }).filter {
      _ != null
    }


    val result = parseRDD
      // 批量上传到HBase，最后产生的结果存入Hdfs中
      .mapPartitions({ f =>
        var lst = List[(String, String)]()

        if (f != null) {
          val conn = HBaseUtil.getConnection(null)
          // 上传到HBase的两个表
          val sourceTable = conn.getTable(TableName.valueOf(HBaseUtil.TABLE_NAME_CPZ_SOURCE_V2))
          // 每批次大小是100
          val sourcePuts = new ArrayList[Put](100)
          val appTable = conn.getTable(TableName.valueOf(HBaseUtil.TABLE_NAME_CPZ_APP_V2))
          val appPuts = new ArrayList[Put](1000)
          while (f.hasNext) {
            val result = f.next
            val cpz = ParseBrush.toSourcePut(result)
            sourcePuts.add(cpz)
            val appList = result.appList
            val appStrList = ArrayBuffer[String]()
            if (appList != null) {
              for (app <- appList) {
                appPuts.add(ParseBrush.toAppPut(app))

                val ll = ArrayBuffer[String]()
                ll += (app.appdetailid)
                ll += (app.pkg)
                ll += (app.areatype)
                //2017-05-10 new add  impandsuc
                ll += (if (app.impandsuc != null) app.impandsuc else "0")
                appStrList += (ll.mkString(","))
              }
            }
            val rowkey = Bytes.toString(cpz.getRow)
            val appStr = appStrList.mkString(";")
            val sb = new StringBuilder
            if (StringUtils.isNotBlank(result.ime1))
              sb.append("," + result.ime1)
            if (StringUtils.isNotBlank(result.ime2))
              sb.append("," + result.ime2)
            val l = ArrayBuffer[String]()
            l += (rowkey)
            l += (if (sb.length > 0) sb.substring(1) else " ")
            l += (result.customid)
            l += (appStr)
            l += (if (StringUtils.isNotBlank(result.phonemodel)) result.phonemodel else "0")
            //2017-05-10 new add
            l += (if (StringUtils.isNotBlank(result.ctype)) result.ctype else "0")
            l += (if (StringUtils.isNotBlank(result.spid)) result.spid else "0")
            l += (if (StringUtils.isNotBlank(result.cdate)) result.cdate else " ")
            l += (if (StringUtils.isNotBlank(result.clienttype)) result.clienttype else "0") //1:pc,2:box
            l += (result.systemappcount)
            l += (result.userappcount)
            l += (if (StringUtils.isNotBlank(result.acc)) result.acc else " ")
            l += (if (StringUtils.isNotBlank(result.scriptmode)) result.scriptmode else "0")
            //romid  romtype supportflash?

            lst = lst.:+((rowkey, l.mkString("&")))

          }
          sourceTable.put(sourcePuts)
          appTable.put(appPuts)

          sourceTable.close()
          appTable.close()
          conn.close()
          //appConn.close()
        }
        lst.iterator
      })
    // /2016/12/14/00/flashtool_2016121400
    val filename = args(0).substring(args(0).lastIndexOf("/") + 1)

    var temp = filename
    var pos = temp.length() - 2
    val hour = temp.substring(pos)
    temp = temp.substring(0, pos)
    pos = temp.length() - 2
    val day = temp.substring(pos)
    temp = temp.substring(0, pos)
    pos = temp.length() - 2
    val month = temp.substring(pos)
    temp = temp.substring(0, pos)
    pos = temp.length() - 4
    val year = temp.substring(pos)

    val cpz_data_hdfs = HdfsUtil.CPZ_DATA_DIR + "/" + year + "/" + month + "/" + day + "/" + hour
    // 存入Hdfs
    result.partitionBy(new HashPartitioner(2)).map(f => f._2).saveAsTextFile(cpz_data_hdfs)

    sc.stop()

    //执行imei入库操作
    val rt = Runtime.getRuntime()
    val cmd = Array("/bin/sh", "-c", "nohup /data/install/spark/bin/spark-submit --master spark://fenxi-xlg:7077 --executor-memory 8G --total-executor-cores 10 --name Data-Analyze --supervise --class com.analysis.test.prd.imei.ExportImeisFromHdfsByDayForCpz /home/hadoop/analysis-0.0.1-SNAPSHOT.jar " + cpz_data_hdfs + " > /home/hadoop/logs/ExportImeisFromHdfsByDayForCpz.out &")

    try {
      val proc = rt.exec(cmd)
      // 获取进程的标准输入流
      proc.waitFor()
      proc.destroy()
      println("装刷imei入库任务提交")
    } catch {
      case e: Exception =>
        println(args(0) + "装刷imei入库任务提交失败：" + e.getMessage)
    }

  }

  //获取初始化基础信息
  def getAllPhoneModelAndBrand(): Map[(String, String), String] = {
    var dbconn: Connection = null
    var stmt: Statement = null
    var rs: ResultSet = null

    var map = Map[(String, String), String]()
    try {
      dbconn = DriverManager.getConnection(DbUtil.ADMIN_DB_URL, DbUtil.ADMIN_DB_USERNAME, DbUtil.ADMIN_DB_PASSWORD)

      stmt = dbconn.createStatement()

      rs = stmt.executeQuery("select a.phonemodelcode, b.manufacturername,max(a.phonemodelid) from t_phonemodel a left join t_phonemanufactur b on a.manufacturerid = b.manufacturerid group by a.phonemodelcode, b.manufacturername");
      while (rs.next()) {
        var model = rs.getString(1)
        if (StringUtils.isNotBlank(model))
          model = model.toUpperCase()
        var brand = rs.getString(2)
        if (StringUtils.isNotBlank(brand))
          brand = brand.toUpperCase()
        val phoneid = rs.getLong(3).toString()
        map += ((model, brand) -> phoneid)
      }
      System.out.println("获取机型成功")
    } catch {
      case e: Exception =>
        System.out.println("获取机型失败:" + e.getMessage)
        println("trace:" + e.printStackTrace())
        System.err.println("异常")
        System.exit(1)
    } finally {
      if (rs != null)
        rs.close()
      if (stmt != null)
        stmt.close()
      if (dbconn != null)
        dbconn.close()
    }
    map
  }

}