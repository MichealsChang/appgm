package com.analysis.install

import java.net.URLDecoder
import java.text.SimpleDateFormat
import java.util.{ArrayList, Date}

import com.analysis.util.{HbaseUtil, HdfsUtil}
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, _}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HBaseAdmin, Put}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark._

import scala.collection.mutable.ArrayBuffer

/**
  * 解析刷机原始日志，结果存入Hdfs和HBase
  */
object ParseBrushFinalSync {
  //不同环境修改
  var conf: Configuration = null
  var conf_app: Configuration = null
  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def getSourceConnection() = {
    if (conf == null) {
      conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum", HbaseUtil.HBASE_ZOOKEEPER_QUORUM)
      conf.set("hbase.zookeeper.property.clientPort", HbaseUtil.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT)
      conf.setInt("hbase.rpc.timeout", 12000000)
      conf.setInt("hbase.client.operation.timeout", 12000000)
      conf.setInt("hbase.client.scanner.timeout.period", 12000000)

      conf.set(TableInputFormat.INPUT_TABLE, HbaseUtil.TABLE_NAME_CPZ_SOURCE_V2)
    }
    ConnectionFactory.createConnection(conf)
  }

  def getAppConnection() = {
    if (conf_app == null) {
      conf_app = HBaseConfiguration.create()
      conf_app.set("hbase.zookeeper.quorum", HbaseUtil.HBASE_ZOOKEEPER_QUORUM)
      conf_app.set("hbase.zookeeper.property.clientPort", HbaseUtil.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT)
      conf_app.setInt("hbase.rpc.timeout", 12000000)
      conf_app.setInt("hbase.client.operation.timeout", 12000000)
      conf_app.setInt("hbase.client.scanner.timeout.period", 12000000)

      conf_app.set(TableInputFormat.INPUT_TABLE, HbaseUtil.TABLE_NAME_CPZ_APP_V2)
    }
    ConnectionFactory.createConnection(conf_app)
  }

  def main(args: Array[String]) {
    //参数格式 2016-09-12 00:00:00 2016-09-12 07:59:59
    if (args == null || args.length < 1) {
      System.err.println("参数异常")
      System.exit(1)
    }
    //System.setProperty("spark.scheduler.mode", "FAIR") 
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sparkConf = new SparkConf().setAppName("ParseBrushFinalSync_V2_2-" + args(0))

    val sc = new SparkContext(sparkConf)

    // HBase表不存在就创建
    var conn = getSourceConnection()
    var admin = new HBaseAdmin(conn)
    if (!admin.isTableAvailable(HbaseUtil.TABLE_NAME_CPZ_SOURCE_V2)) {
      val tableDesc = new HTableDescriptor(
        TableName.valueOf(HbaseUtil.TABLE_NAME_CPZ_SOURCE_V2))
      val hcd = new HColumnDescriptor(HbaseUtil.COLUMN_FAMILY)
      tableDesc.addFamily(hcd)
      admin.createTable(tableDesc)
    }
    admin.close()
    conn.close()

    conn = getAppConnection()
    admin = new HBaseAdmin(conn)
    if (!admin.isTableAvailable(HbaseUtil.TABLE_NAME_CPZ_APP_V2)) {
      val tableDesc = new HTableDescriptor(
        TableName.valueOf(HbaseUtil.TABLE_NAME_CPZ_APP_V2))
      val hcd = new HColumnDescriptor(HbaseUtil.COLUMN_FAMILY)
      tableDesc.addFamily(hcd)
      admin.createTable(tableDesc)
    }
    admin.close()
    conn.close()

    // 读取args(0):/2017/05/03/12/flashtool_2017050312
    val parseRDD = sc.textFile(args(0)).flatMap(f => {
      try {
        // 先解码
        var x = URLDecoder.decode(f, "GBK")
        // 结果存为map形式
        var map = Map[String, String]()
        if (x.contains("clienttype") && x.contains("imei")) {

          val ip = x.substring(0, x.indexOf("|"))
          x = x.substring(x.indexOf("|") + 1)
          // 13位时间戳,接收服务器的接收的时间
          var tss = x.substring(0, x.indexOf("|"))
          tss = tss.replaceAll("\\.", "")
          // var spt = x.split("\\|")
          val ts = tss.toLong
          val posttime = sdf.format(new Date(ts))
          map += ("posttime" -> posttime)

          x = x.substring(x.indexOf("|") + 1)
          val arr = x.split("&")
          for (a <- arr) {
            val t = a.split("=")
            if (t.length == 1) //XX=&YY=c的情况，对XX= split之后长度为1
              map += (t(0) -> "")
            else
              map += (t(0) -> t(1))
          }
          val time = map.getOrElse("time", "")
          // 客户端时间不为空
          if (StringUtils.isNotBlank(time)) {
            var cpzSourceList = List[CpzSource]()
            // 第一个imei
            var imeiStr: String = null

            // 第二个imei
            var imei2Str: String = null
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
              //println(">>>>>>>>>> imeiList size:"+imeiList.length)
              for ((v, i) <- imeiList.zipWithIndex) {
                //println(">>>>>>>>>>>>> i")
                var imei2: String = null
                if (imei2List != null && imei2List.length > i)
                  imei2 = imei2List(i) // 使imei1和imei2顺序配对
                val tcpzsource = ParseBrush.toCpzSource((v, imei2), map)
                //println(">>>>>>>>>>>>> y")
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

    })
      .filter(_ != null)
      //重新分区
      .map(x => {
      ((x.customid, x.cdate, x.ime1, x.ime2), x)
    })
      .partitionBy(new HashPartitioner(20))
      .reduceByKey((a, b) => {
        // 重复的留较早上报的，后续还有去重
        if (a.postdate < b.postdate)
          a
        else
          b
      })
      .map(f => f._2)
      // 得出applist、systemappcount和userappcount
      .map(x => {
        try {
          val packageList = x.packagelist
          // println(">>>>packageList:"+packageList)
          var appList = List[CpzApp]()
          if (StringUtils.isNotBlank(packageList)) {
            val arr = packageList.split(";")
            var systemappcount = 0
            var userappcount = 0
            //println("arr lenght:"+arr.length)
            //1130,cn.ninegame.gamemanager,0ae10fcc8dc137898988f95e410ac323,0,1
            for (a <- arr) {
              if (StringUtils.isNotBlank(a)) {
                //println("a:"+a)
                val appdetail = a.split(",")
                if ("0".equalsIgnoreCase(appdetail(3))) {
                  userappcount = userappcount + 1
                } else if ("1".equalsIgnoreCase(appdetail(3))) {
                  systemappcount = systemappcount + 1
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
                var impandsuc: String = null // 重要且安装成功
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

    // 入HBase
    val result = parseRDD
      // 映射函数的参数由RDD中的每一个元素变成了RDD中每一个分区的迭代器
      .mapPartitions({ f =>
        var lst = List[(String, String)]()

        if (f != null) {
          val sourceConn = getSourceConnection()
          val sourceTable = sourceConn.getTable(TableName.valueOf(HbaseUtil.TABLE_NAME_CPZ_SOURCE_V2))
          val sourcePuts = new ArrayList[Put](100)
          val appConn = getAppConnection()
          val appTable = appConn.getTable(TableName.valueOf(HbaseUtil.TABLE_NAME_CPZ_APP_V2))
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
                appStrList += (app.appdetailid + "," + app.pkg + "," + app.areatype)
              }
            }
            val rowkey = Bytes.toString(cpz.getRow)
            val appStr = appStrList.mkString(";")
            val sb = new StringBuilder()
            if (StringUtils.isNotBlank(result.ime1))
              sb.append("," + result.ime1)
            if (StringUtils.isNotBlank(result.ime2))
              sb.append("," + result.ime2)
            val customid = result.customid
            var phonemodelid = result.phonemodel
            if (StringUtils.isBlank(phonemodelid))
              phonemodelid = "0"
            if (sb.length > 0)
              lst = lst.:+((rowkey, rowkey + "&" + sb.substring(1) + "&" + customid + "&" + appStr + "&" + phonemodelid))
            else
              lst = lst.:+((rowkey, rowkey + "&" + " " + "&" + customid + "&" + appStr + "&" + phonemodelid))

          }
          sourceTable.put(sourcePuts)
          sourceTable.close()
          appTable.put(appPuts)
          appTable.close()
          sourceConn.close()
          appConn.close()
        }
        lst.iterator
      })
    //  /2016/12/14/00/flashtool_2016121400
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

    // 入Hdfs
    val cpz_data_hdfs = HdfsUtil.CPZ_DATA_DIR + "/" + year + "/" + month + "/" + day + "/" + hour
    result
      .partitionBy(new HashPartitioner(2))
      .map(f => f._2)
      .saveAsTextFile(cpz_data_hdfs)


    sc.stop()

    //执行imei入库操作
    val rt = Runtime.getRuntime()
    val cmd = Array("/bin/sh", "-c", "nohup /data/install/spark/bin/spark-submit --master spark://fenxi-xlg:7077 --executor-memory 8G --total-executor-cores 10 --name Data-Analyze --supervise --class com.analysis.test.prd.imei.ExportImeisFromHdfsByDayForCpz --jars /data/install/hbase/lib/hbase-client-1.2.2.jar,/data/install/hbase/lib/hbase-server-1.2.2.jar,/data/install/hbase/lib/hbase-common-1.2.2.jar,/data/install/hbase/lib/hbase-protocol-1.2.2.jar,/data/install/hbase/lib/guava-12.0.1.jar,/data/install/hbase/lib/htrace-core-3.1.0-incubating.jar,/data/install/hbase/lib/metrics-core-2.2.0.jar,/home/hadoop/jars/mysql-connector-java-5.1.25.jar,/home/hadoop/jars/fastjson-1.2.1.jar,/home/hadoop/jars/jedis-2.7.2.jar,/home/hadoop/jars/commons-pool2-2.3.jar,/home/hadoop/jars/pinyin4j-2.5.0.jar,/home/hadoop/jars/ojdbc6.jar /home/hadoop/analysis-0.0.1-SNAPSHOT.jar " + cpz_data_hdfs + " > /home/hadoop/ExportImeisFromHdfsByDayForCpz.out &")

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

}