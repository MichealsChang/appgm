package com.analysis.install

import java.sql.{Connection, DriverManager, ResultSet, Statement, Timestamp}
import java.text.SimpleDateFormat
import java.util.Date

import com.analysis.util.{DbUtil, HbaseUtil, ImeiUtil}
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import scala.util.Random

/**
 * 将刷机数据导入到Mysql
object ExportCpzSourceToMysqlHourAdjustSync {

  def main(args: Array[String]) {
    //新数据hdfs地址  /changmi/cpz_data/2016/12/27/08
    if (args == null || args.length < 1) {
      System.err.println("参数异常")
      System.exit(1)
    }

    var s = args(0)

    var pos = s.lastIndexOf("/")
    val hour = s.substring(pos + 1)
    s = s.substring(0, pos)
    pos = s.lastIndexOf("/")
    val day = s.substring(pos + 1)
    s = s.substring(0, pos)
    pos = s.lastIndexOf("/")
    val month = s.substring(pos + 1)
    s = s.substring(0, pos)
    pos = s.lastIndexOf("/")
    val year = s.substring(pos + 1)
    if (year.length() != 4 || month.length() != 2 || day.length() != 2 || hour.length() != 2) {
      System.err.println("参数格式不规范")
      System.exit(1)
    }

    //System.setProperty("spark.scheduler.mode", "FAIR") 
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sparkConf = new SparkConf().setAppName("ExportCpzSourceToMysqlHourAdjustSync_V2-" + args(0))
    val sc = new SparkContext(sparkConf)

    //检查imei库数据是否准备好并返回最大id
    val maxId = imeiCheck(year, month, day, hour)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val imeis = sqlContext.read.format("jdbc").options(
      Map("url" -> DbUtil.IMEI_DB_URL,
        //"dbtable"-> "(SELECT imei,imeiid,num FROM t_imei) a",
        "dbtable" -> ImeiUtil.IMEI_CPZ_TABLE,
        "user" -> DbUtil.IMEI_DB_USERNAME,
        "password" -> DbUtil.IMEI_DB_PASSWORD,
        "driver" -> "com.mysql.jdbc.Driver", //"com.mysql.jdbc.Driver","oracle.jdbc.driver.OracleDriver"
        //"fetchSize" -> "1000",
        "partitionColumn" -> "id",
        "lowerBound" -> "1",
        "upperBound" -> maxId.toString(),
        "numPartitions" -> "15")).load()

    val imeisRDD = imeis.rdd.map(t => {
      //t(0)  id
      val imei = t(1).toString
      val imeiid = t(2).toString
      val num = t(3).toString.toInt

      (ImeiUtil.parseLength(imei), (imei, imeiid, num))
    })
    
        val conf_pre = HBaseConfiguration.create()
    conf_pre.set("hbase.zookeeper.quorum", HbaseUtil.HBASE_ZOOKEEPER_QUORUM)
    conf_pre.set("hbase.zookeeper.property.clientPort", HbaseUtil.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT)
    conf_pre.setInt("hbase.rpc.timeout", 12000000)
    conf_pre.setInt("hbase.client.operation.timeout", 12000000)
    conf_pre.setInt("hbase.client.scanner.timeout.period", 12000000)
    conf_pre.set(TableInputFormat.INPUT_TABLE, HbaseUtil.TABLE_NAME_CPZ_SOURCE)

    val scan_pre = new Scan()
    scan_pre.setCaching(10000)
    scan_pre.setCacheBlocks(false)
    scan_pre.addColumn(HbaseUtil.COLUMN_FAMILY.getBytes, "postdate".getBytes)
    scan_pre.addColumn(HbaseUtil.COLUMN_FAMILY.getBytes, "phonemodel".getBytes)
    scan_pre.addColumn(HbaseUtil.COLUMN_FAMILY.getBytes, "ctype".getBytes)
    scan_pre.addColumn(HbaseUtil.COLUMN_FAMILY.getBytes, "ime1".getBytes)
    scan_pre.addColumn(HbaseUtil.COLUMN_FAMILY.getBytes, "ime2".getBytes)
    scan_pre.addColumn(HbaseUtil.COLUMN_FAMILY.getBytes, "state".getBytes)
    scan_pre.addColumn(HbaseUtil.COLUMN_FAMILY.getBytes, "spid".getBytes)
    scan_pre.addColumn(HbaseUtil.COLUMN_FAMILY.getBytes, "cdate".getBytes)
    //scan.addColumn(COLUMN_FAMILY.getBytes, "phonemodelname".getBytes)
    scan_pre.addColumn(HbaseUtil.COLUMN_FAMILY.getBytes, "customid".getBytes)
    scan_pre.addColumn(HbaseUtil.COLUMN_FAMILY.getBytes, "systemappcount".getBytes)
    scan_pre.addColumn(HbaseUtil.COLUMN_FAMILY.getBytes, "userappcount".getBytes)
    scan_pre.addColumn(HbaseUtil.COLUMN_FAMILY.getBytes, "clienttype".getBytes)
    conf_pre.set(TableInputFormat.SCAN, HbaseUtil.convertScanToString(scan_pre))

    // 读取HBase数据
    val sourcePreRDD = sc.newAPIHadoopRDD(conf_pre, classOf[TableInputFormat], classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable], classOf[org.apache.hadoop.hbase.client.Result])

    val sourcePre = sourcePreRDD
      .map({
        case (_, result) =>
          val key = Bytes.toString(result.getRow())
          //val ime1 = parseLength(Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "ime1".getBytes)))

          //val ime2 = parseLength(Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "ime2".getBytes)))
          val postdate = Bytes.toString(result.getValue(HbaseUtil.COLUMN_FAMILY.getBytes, "postdate".getBytes))
          //val operationdate = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "operationdate".getBytes))
          //val operationtype = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "operationtype".getBytes))
          //val imid = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "imid".getBytes))
          //val channelid = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "channelid".getBytes))
          val phonemodel = Bytes.toString(result.getValue(HbaseUtil.COLUMN_FAMILY.getBytes, "phonemodel".getBytes))
          //val cooperationtype = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "cooperationtype".getBytes))
          //val cost = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "cost".getBytes))
          val ctype = Bytes.toString(result.getValue(HbaseUtil.COLUMN_FAMILY.getBytes, "ctype".getBytes))
          val ime1 = Bytes.toString(result.getValue(HbaseUtil.COLUMN_FAMILY.getBytes, "ime1".getBytes))
          val ime2 = Bytes.toString(result.getValue(HbaseUtil.COLUMN_FAMILY.getBytes, "ime2".getBytes))
          //val currencytype = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "currencytype".getBytes))
          val state = Bytes.toString(result.getValue(HbaseUtil.COLUMN_FAMILY.getBytes, "state".getBytes))
          val spid = Bytes.toString(result.getValue(HbaseUtil.COLUMN_FAMILY.getBytes, "spid".getBytes))
          //val creatorid = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "creatorid".getBytes))
          //val adddate = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "adddate".getBytes))
          //val ccount = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "ccount".getBytes))
          //val chcount = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "chcount".getBytes))
          //val simstate = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "simstate".getBytes))
          //val linktype = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "linktype".getBytes))
          //val manufacturid = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "manufacturid".getBytes))
          //val manufacturname = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "manufacturname".getBytes))
          val cdate = Bytes.toString(result.getValue(HbaseUtil.COLUMN_FAMILY.getBytes, "cdate".getBytes))
          //val phonemodelname = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "phonemodelname".getBytes))
          //val verid = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "verid".getBytes))
          //val packagelist = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "packagelist".getBytes))
          //val cpzsourceid = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "cpzsourceid".getBytes))
          val customid = Bytes.toString(result.getValue(HbaseUtil.COLUMN_FAMILY.getBytes, "customid".getBytes))
          //val romid = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "romid".getBytes))
          //val romtype = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "romtype".getBytes))
          //val packagelist_copy = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "packagelist_copy".getBytes))
          //val companyid = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "companyid".getBytes))
          val systemappcount = Bytes.toString(result.getValue(HbaseUtil.COLUMN_FAMILY.getBytes, "systemappcount".getBytes))
          val userappcount = Bytes.toString(result.getValue(HbaseUtil.COLUMN_FAMILY.getBytes, "userappcount".getBytes))
          val clienttype = Bytes.toString(result.getValue(HbaseUtil.COLUMN_FAMILY.getBytes, "clienttype".getBytes))
          //val supportflash = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "supportflash".getBytes))

          //        var appcount = 0
          //        if(StringUtils.isNotBlank(packagelist))
          //          appcount = packagelist.split(";").length;
          //        
          val cpzSource =
            //          new CpzSource(
            //      postdate, operationdate, operationtype, imid, channelid, phonemodel, cooperationtype, cost, ctype, ime1, ime2, currencytype, state, spid, creatorid, adddate, ccount, chcount, simstate, linktype, manufacturid, manufacturname, cdate, phonemodelname, verid, packagelist, cpzsourceid, customid, romid, romtype, packagelist_copy, companyid, systemappcount, userappcount, clienttype, supportflash, null)

            new CpzSource(
              postdate, null, null, null, null, phonemodel, null, null, ctype, ime1, ime2, null, state, spid, null, null, null, null, null, null, null, null, cdate, null, null, null, null, customid, null, null, null, null, systemappcount, userappcount, clienttype, null, null)
          cpzSource.key = key
          if (cpzSource.userappcount == null)
            cpzSource.userappcount = "0"
          if (cpzSource.systemappcount == null)
            cpzSource.systemappcount = "0"

          cpzSource.appcount = cpzSource.userappcount.toInt + cpzSource.systemappcount.toInt
          cpzSource.isNew = 0

          (key, cpzSource)
      })

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", HbaseUtil.HBASE_ZOOKEEPER_QUORUM)
    conf.set("hbase.zookeeper.property.clientPort", HbaseUtil.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT)
    conf.setInt("hbase.rpc.timeout", 12000000)
    conf.setInt("hbase.client.operation.timeout", 12000000)
    conf.setInt("hbase.client.scanner.timeout.period", 12000000)

    conf.set(TableInputFormat.INPUT_TABLE, HbaseUtil.TABLE_NAME_CPZ_SOURCE_V2)

    //    //获取指定时段的数据
    //    val start_time = args(0) + " " + args(1) + ".000"
    //    val end_time = args(2) + " " + args(3) + ".999"

    //import java.text.SimpleDateFormat
    //val sdf = new SimpleDateFormat("yyyy-MM-dd H:mm:ss.SSS")

    //val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //    val start_timestamp = sdf.parse(start_time).getTime.toString()
    //    val end_timestamp = sdf.parse(end_time).getTime.toString()
    //    //时间区域+二次时间间隔判断(分钟为单位)
    //    val params = List[String](
    //      start_time.replace("-", "").replace(" ", "").replace(":", "").replace(".", ""), end_time.replace("-", "").replace(" ", "").replace(":", "").replace(".", ""), args(4).toString)
    //    println(">>>>>>>>>>>>>>>>>>>>start_time:" + start_time + " end_time:" + end_time)
    //    println(">>>>>>>>>>>>>>>>>>>>start_timestamp:" + start_timestamp + " end_timestamp:" + end_timestamp)
    //
    //    val broadInstance = sc.broadcast(params)

    val scan = new Scan()
    scan.setCaching(10000)
    scan.setCacheBlocks(false)
    scan.addColumn(HbaseUtil.COLUMN_FAMILY.getBytes, "postdate".getBytes)
    scan.addColumn(HbaseUtil.COLUMN_FAMILY.getBytes, "phonemodel".getBytes)
    scan.addColumn(HbaseUtil.COLUMN_FAMILY.getBytes, "ctype".getBytes)
    scan.addColumn(HbaseUtil.COLUMN_FAMILY.getBytes, "ime1".getBytes)
    scan.addColumn(HbaseUtil.COLUMN_FAMILY.getBytes, "ime2".getBytes)
    scan.addColumn(HbaseUtil.COLUMN_FAMILY.getBytes, "state".getBytes)
    scan.addColumn(HbaseUtil.COLUMN_FAMILY.getBytes, "spid".getBytes)
    scan.addColumn(HbaseUtil.COLUMN_FAMILY.getBytes, "cdate".getBytes)
    //scan.addColumn(COLUMN_FAMILY.getBytes, "phonemodelname".getBytes)
    scan.addColumn(HbaseUtil.COLUMN_FAMILY.getBytes, "customid".getBytes)
    scan.addColumn(HbaseUtil.COLUMN_FAMILY.getBytes, "systemappcount".getBytes)
    scan.addColumn(HbaseUtil.COLUMN_FAMILY.getBytes, "userappcount".getBytes)
    scan.addColumn(HbaseUtil.COLUMN_FAMILY.getBytes, "clienttype".getBytes)
    conf.set(TableInputFormat.SCAN, HbaseUtil.convertScanToString(scan))

    val sourceRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable], classOf[org.apache.hadoop.hbase.client.Result])

    val source = sourceRDD
      .map({
        case (_, result) =>
          val key = Bytes.toString(result.getRow())
          //val ime1 = parseLength(Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "ime1".getBytes)))

          //val ime2 = parseLength(Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "ime2".getBytes)))
          val postdate = Bytes.toString(result.getValue(HbaseUtil.COLUMN_FAMILY.getBytes, "postdate".getBytes))
          //val operationdate = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "operationdate".getBytes))
          //val operationtype = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "operationtype".getBytes))
          //val imid = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "imid".getBytes))
          //val channelid = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "channelid".getBytes))
          val phonemodel = Bytes.toString(result.getValue(HbaseUtil.COLUMN_FAMILY.getBytes, "phonemodel".getBytes))
          //val cooperationtype = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "cooperationtype".getBytes))
          //val cost = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "cost".getBytes))
          val ctype = Bytes.toString(result.getValue(HbaseUtil.COLUMN_FAMILY.getBytes, "ctype".getBytes))
          val ime1 = Bytes.toString(result.getValue(HbaseUtil.COLUMN_FAMILY.getBytes, "ime1".getBytes))
          val ime2 = Bytes.toString(result.getValue(HbaseUtil.COLUMN_FAMILY.getBytes, "ime2".getBytes))
          //val currencytype = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "currencytype".getBytes))
          val state = Bytes.toString(result.getValue(HbaseUtil.COLUMN_FAMILY.getBytes, "state".getBytes))
          val spid = Bytes.toString(result.getValue(HbaseUtil.COLUMN_FAMILY.getBytes, "spid".getBytes))
          //val creatorid = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "creatorid".getBytes))
          //val adddate = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "adddate".getBytes))
          //val ccount = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "ccount".getBytes))
          //val chcount = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "chcount".getBytes))
          //val simstate = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "simstate".getBytes))
          //val linktype = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "linktype".getBytes))
          //val manufacturid = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "manufacturid".getBytes))
          //val manufacturname = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "manufacturname".getBytes))
          val cdate = Bytes.toString(result.getValue(HbaseUtil.COLUMN_FAMILY.getBytes, "cdate".getBytes))
          //val phonemodelname = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "phonemodelname".getBytes))
          //val verid = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "verid".getBytes))
          //val packagelist = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "packagelist".getBytes))
          //val cpzsourceid = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "cpzsourceid".getBytes))
          val customid = Bytes.toString(result.getValue(HbaseUtil.COLUMN_FAMILY.getBytes, "customid".getBytes))
          //val romid = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "romid".getBytes))
          //val romtype = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "romtype".getBytes))
          //val packagelist_copy = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "packagelist_copy".getBytes))
          //val companyid = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "companyid".getBytes))
          val systemappcount = Bytes.toString(result.getValue(HbaseUtil.COLUMN_FAMILY.getBytes, "systemappcount".getBytes))
          val userappcount = Bytes.toString(result.getValue(HbaseUtil.COLUMN_FAMILY.getBytes, "userappcount".getBytes))
          val clienttype = Bytes.toString(result.getValue(HbaseUtil.COLUMN_FAMILY.getBytes, "clienttype".getBytes))
          //val supportflash = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "supportflash".getBytes))

          //        var appcount = 0
          //        if(StringUtils.isNotBlank(packagelist))
          //          appcount = packagelist.split(";").length;
          //        
          val cpzSource =
            //          new CpzSource(
            //      postdate, operationdate, operationtype, imid, channelid, phonemodel, cooperationtype, cost, ctype, ime1, ime2, currencytype, state, spid, creatorid, adddate, ccount, chcount, simstate, linktype, manufacturid, manufacturname, cdate, phonemodelname, verid, packagelist, cpzsourceid, customid, romid, romtype, packagelist_copy, companyid, systemappcount, userappcount, clienttype, supportflash, null)

            new CpzSource(
              postdate, null, null, null, null, phonemodel, null, null, ctype, ime1, ime2, null, state, spid, null, null, null, null, null, null, null, null, cdate, null, null, null, null, customid, null, null, null, null, systemappcount, userappcount, clienttype, null, null)
          cpzSource.key = key
          if (cpzSource.userappcount == null)
            cpzSource.userappcount = "0"
          if (cpzSource.systemappcount == null)
            cpzSource.systemappcount = "0"

          cpzSource.appcount = cpzSource.userappcount.toInt + cpzSource.systemappcount.toInt
          cpzSource.isNew = 0

          (key, cpzSource)
      }).union(sourcePre)//合并老的,防止老的重复上报
      .partitionBy(new HashPartitioner(150))
    val newRowkeys = sc.textFile(args(0))
      .map(f => {
        // rowkey&ime1,ime2&customid&apps
        try {
          val arr = f.split("&")
          if (arr.length > 0)
            (arr(0), 1)
          else
            null
        } catch {
          case e: Exception =>
            null
        }
      }).filter(_!=null)
//    val newData = source.join(newRowkeys)
//      .map(f => {
//        val cpz = f._2._1
//        cpz.isNew = 1
//        (f._1, f._2._1)
//      })
//    val preData = source.subtractByKey(newRowkeys)
//
//    val allData = newData.union(preData)
    val allData = source.leftOuterJoin(newRowkeys)
    .map(f=>{
      val key = f._1
      val cpzSource = f._2._1
      val param = f._2._2
      if(!param.isEmpty)
        cpzSource.isNew = 1
      (key,cpzSource)
    })
    //去除重复上报的数据
    .map(f=>{
      (f._2.postdate,f)
    })//.partitionBy(new HashPartitioner(50))
    
    .map(f=>{
      val imeis = f._2._1.substring(0, 32)
      val uid = f._2._2.customid
      val cdate = f._2._2.cdate
      ((uid,imeis,cdate),f._2)
    })
    .reduceByKey((a,b)=>{
      //留一条，服务器时间早的
      val aposttime = a._2.postdate
      val bposttime = b._2.postdate
      if(aposttime<bposttime)
        a
      else
        b
    })
    .map(f=>{
      f._2
    })
    
    
      .flatMap(f => {
        val cpzSource = f._2
        var list = List[(String, CpzSource)]()
        if (StringUtils.isNotBlank(cpzSource.ime1))
          list = list.:+(ImeiUtil.parseLength(cpzSource.ime1), cpzSource)
        if (StringUtils.isNotBlank(cpzSource.ime2))
          list = list.:+(ImeiUtil.parseLength(cpzSource.ime2), cpzSource)
        list
      })

    val sourceJoin = allData.join(imeisRDD)
      .map(f => {
        val sourcedata = f._2._1
        val imeiData = f._2._2
        val imeiid = imeiData._2
        //key (imeiid sourcedata)
        (sourcedata.key, (imeiid, sourcedata))
      }).reduceByKey((a, b) => {
        //相同key,imeiid,sourcedata一样
        (a._1, a._2)
      })
      .map(f => {
        val imeiid = f._2._1
        val sourcedata = f._2._2
        //imei,(imeis,sourcedata)
        (imeiid, sourcedata :: Nil)
      })

    val outRDD = sourceJoin
      .reduceByKey((a, b) => {
        a ::: b
      })
      //去除异常数据
      .flatMap(f => {
        val list = f._2.toList.sortWith((a, b) => a.postdate < b.postdate)
        if(list(0).cdate == null) 
          println("first null:"+list(0).key)
        var validList = List[(String, CpzSource)]((f._1, list(0)))
        for ((v, i) <- list.zipWithIndex) {
          //          if(f._1._1=="8628200302674940" && f._1._2 == "8628200302674860")
          //            println("first>>>>>>>>>>>>x:"+v.ime1+" "+v.ime2+" "+v.customid+" "+v.cdate+" "+v.postdate+" "+v.state)
          
          //val postdate = v._1
          val cdate = v.cdate
          if(cdate == null)
            println("other null:"+v.key)
          if (i > 0 && cdate > list(i - 1).cdate)
            validList = validList.:+(f._1, v)
        }
        validList
      })

      //
      .map(f => {
        //(customid,imeiid),cpzSource
        ((f._2.customid, f._1), f._2)
      })
      //      .reduceByKey((a,b)=>{
      //        a
      //      })

      //设置有效无效标识
      .combineByKey(
        (v: CpzSource) => {
          //(v.cdate,v.phonemodel,v.phonemodelname,v.ctype,v.clienttype,v.customid,v.state,1,v.appcount,v.userappcount.toInt,v.systemappcount.toInt)

          (List[CpzSource](v), (0, v.cdate)) //(list,(minIndex,minCdate))

        },
        (acc: (List[CpzSource], (Int, String)), v: CpzSource) => {

          if (v.cdate < acc._2._2) {
            //v的state都默认为“1”
            acc._1(acc._2._1).state = "0"
            val minIndex = acc._1.length
            (acc._1.:+(v), (minIndex, v.cdate))

          } else {
            v.state = "0"
            (acc._1.:+(v), acc._2)
          }
        },
        (acc1: (List[CpzSource], (Int, String)), acc2: (List[CpzSource], (Int, String))) => {
          //比较两者最小时间
          if (acc1._2._2 < acc2._2._2) {
            acc2._1(acc2._2._1).state = "0"
            (acc1._1 ::: acc2._1, acc1._2)
          } else {
            acc1._1(acc1._2._1).state = "0"
            (acc2._1 ::: acc1._1, acc2._2)
          }
        })

      .flatMap(f => {
        var list = List[CpzSource]()
        for ((v, i) <- f._2._1.zipWithIndex) {
          //过滤出新增的数据
          if (v.isNew == 1) {
            list = list.:+(v)
          }
        }
        list
      })
      .map(v => {
        try{
          (v.cdate.substring(0, 10), v)
        }catch{
          case e:Exception=>
            null
        }
      }).filter(f=>(f!=null))
      .map(v=>{
        v._2
      })
      
    //随机处理数据
    var adjustRDD: RDD[(String, List[com.analysis.install.CpzSource])] = null
    val needAjustMap = getNeedAdjust(year, month, day)

    if (needAjustMap != null && needAjustMap.size > 0) {
      val broadInstance = sc.broadcast(needAjustMap)

      adjustRDD = outRDD
      .map(f => {
        (f.customid, f :: Nil)
      }).reduceByKey(_ ::: _)
      .map(f => {
        val customid = f._1
        val needAjustMap = broadInstance.value
        if (needAjustMap.contains(customid)) {
          val cpzList = f._2
          val ratio = needAjustMap.get(customid).get / 100
          //val leftLength = cpzList.length - (cpzList.length * ratio).toInt
          val toInvalidCount = (cpzList.length * ratio).toInt
          var count = 0
          
          //println("customid:"+customid+" ratio:"+ratio+" preLength:"+cpzList.length+" leftLength:"+leftLength)
          //var newList = List[CpzSource]()
          for ((v, i) <- cpzList.zipWithIndex) {
            //将有效的改为无效，个数为toInvalidCount
            if(v.state == "1"&& count<toInvalidCount){
              v.state = "0"
              count = count+1
            }
          }
        }
        f
      })
    } else
      adjustRDD = outRDD.map(f => {
        (f.customid, f :: Nil)
      }).reduceByKey(_ ::: _)
    //var leftRDD = outRDD

    val finalAdjustRDD = adjustRDD.flatMap(f => {
      var list = List[CpzSource]()
      for (v <- f._2) {
        list = list.:+(v)
      }
      list
    })
      .map(v => {
        try{
          (v.cdate.substring(0, 10), v)
        }catch{
          case e:Exception=>
            null
        }
      }).filter(f=>(f!=null))
      .map(v=>{
        v._2
      })

    val adjustDetailList = finalAdjustRDD.collect();

    val adjustStatRDD = finalAdjustRDD
      .map(v => {
          ((v.cdate.substring(0, 10), v.phonemodel, v.ctype, v.clienttype, v.customid, v.userappcount.toInt, v.systemappcount.toInt), (v.state, v.appcount))
      })
      .combineByKey(
        (v: (String, Int)) => {
          //(v.cdate,v.phonemodel,v.phonemodelname,v.ctype,v.clienttype,v.customid,v.state,1,v.appcount,v.userappcount.toInt,v.systemappcount.toInt)

          if ("1".equals(v._1)) {
            (0, 1, v._2) //（无效量，有效量，软件总量）
          } else {
            (1, 0, 0) //无效的不统计软件总量
          }
        },
        (acc: (Int, Int, Int), v: (String, Int)) => {

          if ("1".equals(v._1)) {
            (acc._1, acc._2 + 1, acc._3 + v._2) //（无效量，有效量，软件总量）
          } else {
            (acc._1 + 1, acc._2, acc._3) //无效的不统计软件总量
          }
        },
        (acc1: (Int, Int, Int), acc2: (Int, Int, Int)) => {
          //比较两者最小时间
          (acc1._1 + acc2._1, acc1._2 + acc2._2, acc1._3 + acc2._3)
        })
      .map(f => {
        if (f._2._2 == 0)
          //((cdate,phonemodel,ctype,clienttype,customid,userappcount,systemappcount),(invalid,valid,total,apptotal,average))
          (f._1, (f._2._1, f._2._2, f._2._1 + f._2._2, f._2._3, 0))
        else
          (f._1, (f._2._1, f._2._2, f._2._1 + f._2._2, f._2._3, (f._2._3 / f._2._2)))
      })

    val adjustStat = adjustStatRDD.collect()

    
    val noAdjustDetailList = outRDD.collect();
    
    val noAdjustStatRDD = outRDD
      .map(v => {
          ((v.cdate.substring(0, 10), v.phonemodel, v.ctype, v.clienttype, v.customid, v.userappcount.toInt, v.systemappcount.toInt), (v.state, v.appcount))
      })
      .combineByKey(
        (v: (String, Int)) => {
          //(v.cdate,v.phonemodel,v.phonemodelname,v.ctype,v.clienttype,v.customid,v.state,1,v.appcount,v.userappcount.toInt,v.systemappcount.toInt)

          if ("1".equals(v._1)) {
            (0, 1, v._2) //（无效量，有效量，软件总量）
          } else {
            (1, 0, 0) //无效的不统计软件总量
          }
        },
        (acc: (Int, Int, Int), v: (String, Int)) => {

          if ("1".equals(v._1)) {
            (acc._1, acc._2 + 1, acc._3 + v._2) //（无效量，有效量，软件总量）
          } else {
            (acc._1 + 1, acc._2, acc._3) //无效的不统计软件总量
          }
        },
        (acc1: (Int, Int, Int), acc2: (Int, Int, Int)) => {
          //比较两者最小时间
          (acc1._1 + acc2._1, acc1._2 + acc2._2, acc1._3 + acc2._3)
        })
      .map(f => {
        if (f._2._2 == 0)
          //((cdate,phonemodel,ctype,clienttype,customid,userappcount,systemappcount),(invalid,valid,total,apptotal,average))
          (f._1, (f._2._1, f._2._2, f._2._1 + f._2._2, f._2._3, 0))
        else
          (f._1, (f._2._1, f._2._2, f._2._1 + f._2._2, f._2._3, (f._2._3 / f._2._2)))
      })

    val noAdjustStat = noAdjustStatRDD.collect()
    
    
    
    sc.stop()

    val insMap = getIns(sc)
    //调整的数据入库
    dataToDB(adjustDetailList,adjustStat,insMap,"t_cpzsource","t_cpzsourceresult")
    //未调整的数据入库
    dataToDB(noAdjustDetailList,noAdjustStat,insMap,"t_cpzsource_noadjust","t_cpzsourceresult_noadjust")

  }
  def dataToDB(detailList: Array[CpzSource],statList: Array[((String, String, String, String, String, Int, Int), (Int, Int, Int, Int, Int))],insMap: Map[Int, String],detailTableName:String,statTableName:String)={
    import java.sql.{Connection, DriverManager, PreparedStatement}
    var dbconn: Connection = null
    var ps: PreparedStatement = null
    var dps: PreparedStatement = null

    val detail_sql = "insert into "+detailTableName+"(cdate,phonemodelid,ctype,clienttype,customid,ime1,ime2,instructionname,state,createtime) values (?,?,?,?,?,?,?,?,?,?)"
    
    //T_CpzSourceResult
    val sql = "insert into "+statTableName+"(cdate,phonemodelid,ctype,clienttype,customid,invalidcount,validcount,totalcount,apptotalcount,appaveragecount,systemappcount,userappcount,createtime,dataversion) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

    try {
      dbconn = DriverManager.getConnection(DbUtil.CHANNEL_DB_URL, DbUtil.CHANNEL_DB_USERNAME, DbUtil.CHANNEL_DB_PASSWORD)
      dps = dbconn.prepareStatement(detail_sql)
      ps = dbconn.prepareStatement(sql)
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      val sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val now = new Timestamp(new Date().getTime)
      
      detailList.foreach(f=>{
        val cpz = f
         dps.setTimestamp(1, new Timestamp(sdf2.parse(cpz.cdate).getTime))
         if(cpz.phonemodel!=null)
           dps.setInt(2, cpz.phonemodel.toInt)
         else
           dps.setInt(2, 0)
         if(cpz.ctype!=null)
           dps.setInt(3, cpz.ctype.toInt)
         else
           dps.setInt(3, 0)
         if(cpz.clienttype!=null)
           dps.setInt(4, cpz.clienttype.toInt)
         else
           dps.setInt(4, 0)
         if(cpz.customid!=null)
           dps.setInt(5, cpz.customid.toInt)
         else
           dps.setInt(5, 0)
         dps.setString(6, cpz.ime1)
         dps.setString(7, cpz.ime2)
         if(cpz.spid!=null)
           dps.setString(8, insMap.getOrElse(cpz.spid.toInt, ""))
         else
           dps.setString(8, "")
         dps.setString(9, cpz.state)
         dps.setTimestamp(10, now)
         dps.addBatch()
      })
      statList.foreach(f => {
        val result = f
        //((cdate,phonemodel,ctype,clienttype,customid,userappcount,systemappcount),(invalid,valid,total,apptotal,average))
        ps.setTimestamp(1, new Timestamp(sdf.parse(result._1._1).getTime))
        if(result._1._2!=null)
          ps.setInt(2, result._1._2.toInt)
        else
          ps.setInt(2, 0)
        if(result._1._3!=null)
          ps.setInt(3, result._1._3.toInt)
        else
          ps.setInt(3, 0)
        if(result._1._4!=null)
          ps.setInt(4, result._1._4.toInt)
        else
          ps.setInt(4, 0)
        if(result._1._5!=null)
          ps.setInt(5, result._1._5.toInt)
        else
          ps.setInt(5, 0)
        ps.setInt(6, result._2._1)
        ps.setInt(7, result._2._2)
        ps.setInt(8, result._2._3)
        ps.setInt(9, result._2._4)
        ps.setInt(10, result._2._5)
        ps.setInt(11, result._1._7)
        ps.setInt(12, result._1._6)
        ps.setTimestamp(13, now)
        ps.setString(14, "V2")
        ps.addBatch()
      })
      dbconn.setAutoCommit(false)
      dps.executeBatch()
      ps.executeBatch()
      dbconn.commit()
      dbconn.setAutoCommit(true)

    } catch {
      case e: Exception =>
        println("Sql Exception:" + e.getMessage)
        if (!dbconn.isClosed()) {
          //提交失败，执行回滚操作  
          dbconn.rollback();
          dbconn.setAutoCommit(true)
        }
    } finally {
      if (dps != null)
        dps.close()
      if (ps != null)
        ps.close()

      if (dbconn != null)
        dbconn.close()
    }
    
    
  }
  

  def imeiCheck(year: String, month: String, day: String, hour: String): Long = {
    //检查imei库数据是否准备好
    //Class.forName("com.mysql.jdbc.Driver"); // "oracle.jdbc.driver.OracleDriver"

    var con: Connection = null
    var stmt: Statement = null
    var rs: ResultSet = null
    var rsCheck: ResultSet = null
    var maxId: Long = 0
    try {
      con = DriverManager.getConnection(DbUtil.IMEI_DB_URL, DbUtil.IMEI_DB_USERNAME, DbUtil.IMEI_DB_PASSWORD);
      stmt = con.createStatement();
      rs = stmt.executeQuery("select * from t_imei_log where timeflag = '" + year + month + day + hour + "' and type = 2")
      if (!rs.next()) {
        System.err.println("IMEI库数据未准备好")
        System.exit(1)
      }
      rsCheck = stmt.executeQuery("select max(id) from " + ImeiUtil.IMEI_CPZ_TABLE);
      while (rsCheck.next()) {
        maxId = rsCheck.getLong(1);

      }
    } catch {
      case e: Exception =>
        System.out.println("查询imei最大id失败");
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

  def getNeedAdjust(year: String, month: String, day: String): Map[String, Double] = {
    //检查imei库数据是否准备好

    val adjustConfMap = AdjustUtil.getAdjustConf()
    var map = Map[String, Double]()
    var con: Connection = null
    var stmt: Statement = null
    var rs: ResultSet = null

    try {
      con = DriverManager.getConnection(DbUtil.CHANNEL_DB_URL, DbUtil.CHANNEL_DB_USERNAME, DbUtil.CHANNEL_DB_PASSWORD);
      stmt = con.createStatement();
      rs = stmt.executeQuery("select customid,sum(validcount) from t_cpzsourceresult where cdate like '" + year + "-" + month + "-" + day + "%' group by customid")

      while (rs.next()) {
        val customid = rs.getInt(1)
        val count = rs.getLong(2)
        //如果是需要调整的
        if(adjustConfMap.contains(customid)){
          val adjust = adjustConfMap.get(customid).get
          if (count > adjust.cpzthreshold) {
            val ratio = (new Random().nextInt(adjust.cpzratioupperbound - adjust.cpzratiolowerbound + 1) + adjust.cpzratiolowerbound).toDouble
            map += (customid.toString -> ratio)
  
          }
        }
      }
      map
    } catch {
      case e: Exception =>
        System.out.println("获取需要调整的客户列表失败");
        null
    } finally {
      if (rs != null)
        rs.close()
      if (stmt != null)
        stmt.close()
      if (con != null)
        con.close()
    }
  }
*
   * 获取指令单数据
  def getIns(sc:SparkContext):Map[Int, String]={
    //获取基础数据
    //指令单集合
    var insMap = Map[Int,String]();
    var dbconn: Connection = null
    var stmt:Statement  = null
    var insRs:ResultSet = null
    try{
      dbconn = DriverManager.getConnection(DbUtil.ADMIN_DB_URL, DbUtil.ADMIN_DB_USERNAME, DbUtil.ADMIN_DB_PASSWORD)
      stmt = dbconn.createStatement()
   
      insRs = stmt
  				.executeQuery("select i.inid,i.instructionname from t_instruction i");
  		while (insRs.next()) {
  			val inid = insRs.getInt(1)
  			val instructionname = insRs.getString(2)
  			insMap += (inid -> instructionname)
  		}
    }catch{
      case e:Exception=>
        println(">>>>>>>>>>>>读取基础数据失败")
    }finally{
      if(insRs!=null)
        insRs.close()
      if(stmt!=null)
        stmt.close()
      if(dbconn!=null)
        dbconn.close()
    }
    
    insMap
  }


}*/
