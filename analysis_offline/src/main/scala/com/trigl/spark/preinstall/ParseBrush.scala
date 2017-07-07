package com.trigl.spark.preinstall

import java.util.{ArrayList, Date, UUID}

import com.trigl.spark.entity.{CpzApp, CpzSource}
import com.trigl.spark.utils.RandomUtil
import org.apache.commons.lang.StringUtils
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{ConnectionFactory, HBaseAdmin, Put, Scan}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{FileSplit, InputSplit, TextInputFormat}
import org.apache.spark.rdd.HadoopRDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.Set

/**
  * 解析刷机数据，老版本
  */
object ParseBrush {
  //不同环境修改
  val MASTER_HOSTNAME = "fenxi-hby";

  val SOURCE_TABLE_NAME = "cpz_source"
  val APP_TABLE_NAME = "cpz_app"
  val SOURCE_FILE = "hdfs://" + MASTER_HOSTNAME + ":9000";
  val HBASE_ZOOKEEPER_QUORUM = MASTER_HOSTNAME
  val HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT = "2181"
  val COLUMN_FAMILY = "scf"
  val databytes = Bytes.toBytes(COLUMN_FAMILY)

  val DB_URL = "jdbc:oracle:thin:@114.55.30.70:1521:orcl"
  val DB_USERNAME = "hztp"
  val DB_PASSWORD = "hztp123"
  val NULL_IMEI = "0000000000000000"
  var conf: Configuration = null
  var conf_app: Configuration = null
  //val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val sdf = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  def getSourceConnection() = {
    if (conf == null) {
      conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum", HBASE_ZOOKEEPER_QUORUM)
      conf.set("hbase.zookeeper.property.clientPort", HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT)
      conf.setInt("hbase.rpc.timeout", 12000000)
      conf.setInt("hbase.client.operation.timeout", 12000000)
      conf.setInt("hbase.client.scanner.timeout.period", 12000000)

      conf.set(TableInputFormat.INPUT_TABLE, SOURCE_TABLE_NAME)
    }
    ConnectionFactory.createConnection(conf);
  }

  def getAppConnection() = {
    if (conf_app == null) {
      conf_app = HBaseConfiguration.create()
      conf_app.set("hbase.zookeeper.quorum", HBASE_ZOOKEEPER_QUORUM)
      conf_app.set("hbase.zookeeper.property.clientPort", HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT)
      conf_app.setInt("hbase.rpc.timeout", 12000000)
      conf_app.setInt("hbase.client.operation.timeout", 12000000)
      conf_app.setInt("hbase.client.scanner.timeout.period", 12000000)

      conf_app.set(TableInputFormat.INPUT_TABLE, APP_TABLE_NAME)
    }
    ConnectionFactory.createConnection(conf_app);
  }

  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

  def parseLength(str: String): String = {
    var s = ""
    if (str != null)
      s = str
    val sb = new StringBuilder(s);
    while (sb.length < 16) {
      sb.append("0");
    }
    sb.toString().substring(0, 16);
  }

  def toImei(param: String) = {
    var imei = ""
    if (param.startsWith("99") || param.startsWith("a") || param.startsWith("A"))
      imei = param.substring(0, 14)
    else imei = param.substring(0, 15)
    imei
  }

  def main(args: Array[String]) {
    //参数格式 2016-09-12 00:00:00 2016-09-12 07:59:59
    if (args == null || args.length < 1) {
      System.err.println("参数异常")
      System.exit(1)
    }
    //System.setProperty("spark.scheduler.mode", "FAIR") 
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sparkConf = new SparkConf().setAppName("ParseBrush-" + args(0))

    val sc = new SparkContext(sparkConf)

    var conn = getSourceConnection()
    var admin = new HBaseAdmin(conn);
    if (!admin.isTableAvailable(SOURCE_TABLE_NAME)) {
      val tableDesc = new HTableDescriptor(
        TableName.valueOf(SOURCE_TABLE_NAME));
      val hcd = new HColumnDescriptor(COLUMN_FAMILY);
      tableDesc.addFamily(hcd);
      admin.createTable(tableDesc);
    }
    admin.close()
    conn.close()

    conn = getAppConnection()
    admin = new HBaseAdmin(conn)
    if (!admin.isTableAvailable(APP_TABLE_NAME)) {
      val tableDesc = new HTableDescriptor(
        TableName.valueOf(APP_TABLE_NAME));
      val hcd = new HColumnDescriptor(COLUMN_FAMILY);
      tableDesc.addFamily(hcd);
      admin.createTable(tableDesc);
    }
    admin.close();
    conn.close();
    //    val conn = getAppConnection()
    //    val admin = new HBaseAdmin(conn);
    //    if (!admin.isTableAvailable(SOURCE_TABLE_NAME)) {
    //      val tableDesc = new HTableDescriptor(
    //        TableName.valueOf(SOURCE_TABLE_NAME));
    //      val hcd = new HColumnDescriptor(COLUMN_FAMILY);
    //      tableDesc.addFamily(hcd);
    //      admin.createTable(tableDesc);
    //    }
    //    admin.close();
    //    conn.close();
    var filepaths = Set[String]()
    for (a <- args) {
      filepaths.add(SOURCE_FILE + a)
    }
    val fileRDD = sc.hadoopFile[LongWritable, Text, TextInputFormat](filepaths.mkString(","))
    val hadoopRdd = fileRDD.asInstanceOf[HadoopRDD[LongWritable, Text]]
    val fileAndLine = hadoopRdd.mapPartitionsWithInputSplit((inputSplit: InputSplit, iterator: Iterator[(LongWritable, Text)]) => {
      val file = inputSplit.asInstanceOf[FileSplit]
      iterator.map(x => {
        (file.getPath.toString(), x._2.toString)
      })
    })
    //val lines = sc.textFile(SOURCE_FILE + args(0))
    //    
    //    lines.map ( x => {
    //      println(x)  
    //      x
    //    }).collect().foreach { x => println(x) }
    //    

    val parseRDD = fileAndLine.flatMap(x => {
      var map = Map[String, String]()
      if (x._2.contains("clienttype") && x._2.contains("posttime")) {
        try {
          val arr = x._2.split("&")
          //println("yes")
          for (a <- arr) {
            val t = a.split("=")
            if (t.length == 1) //XX=&YY=c的情况，对XX= split之后长度为1
              map += (t(0) -> "")
            else
              map += (t(0) -> t(1))
          }
          var cpzSourceList = List[CpzSource]()
          var imeiStr: String = null

          var imei2Str: String = null
          var param = map.get("imei")
          if (!param.isEmpty)
            imeiStr = param.get
          //println("imeiStr:"+imeiStr)
          param = map.get("imei2")
          if (!param.isEmpty)
            imei2Str = param.get
          //println("imeiStr:"+imeiStr)
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
                imei2 = imei2List(i)
              val tcpzsource = toCpzSource((v, imei2), map)
              //println(">>>>>>>>>>>>> y")
              cpzSourceList = cpzSourceList.:+(tcpzsource)
            }
          }

          //println(">>>>cpzSourceList size:"+cpzSourceList.length)
          //("1", map) //(处理结果，数据)
          cpzSourceList
        } catch {
          case e: Exception =>
            null :: Nil
        }
      } else {
        null :: Nil
        //        //过滤出异常换行的数据
        //        map += ("filename" -> x._1)
        //        map += ("data" -> x._2)
        //        ("0", map)
      }

    })

      //println(">>>>>>>>>>>>>>>>>>>>count:"+parseRDD.count())

      .filter(f => {
      if (f != null)
        true
      else false
    }).map(x => {
      val packageList = x.packagelist
      // println(">>>>packageList:"+packageList)
      var appList = List[CpzApp]()
      if (StringUtils.isNotBlank(packageList)) {
        val arr = packageList.split(";")
        var systemappcount = 0
        var userappcount = 0
        //println("arr lenght:"+arr.length)
        //1130,cn.ninegame.gamemanager,0ae10fcc8dc137898988f95e410ac323,0
        for (a <- arr) {
          if (StringUtils.isNotBlank(a)) {
            //println("a:"+a)
            val appdetail = a.split(",")
            if ("0".equalsIgnoreCase(appdetail(3))) {
              userappcount = userappcount + 1;
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
            val cdate: String = x.cdate
            val ime1: String = x.ime1
            val ime2: String = x.ime2
            val pkg: String = appdetail(1)
            val app = new CpzApp(appid, appdetailid, imid, state, adddate, cpzasourceid, areatype, cdate, ime1, ime2)
            app.pkg = pkg
            //println("ssssssssssssssssssss")
            appList = appList.:+(app)
          }
        }
        x.systemappcount = systemappcount.toString
        x.userappcount = userappcount.toString
        x.appList = appList
        //println("applist size:"+x.appList.length)
      }
      x
    }).foreachPartition({ f =>
      if (f != null) {
        val sourceConn = getSourceConnection()
        val sourceTable = sourceConn.getTable(TableName.valueOf(SOURCE_TABLE_NAME))
        val sourcePuts = new ArrayList[Put](100)
        val appConn = getAppConnection()
        val appTable = appConn.getTable(TableName.valueOf(APP_TABLE_NAME))
        val appPuts = new ArrayList[Put](1000)
        while (f.hasNext) {
          val result = f.next
          sourcePuts.add(toSourcePut(result))
          val appList = result.appList
          if (appList != null) {
            for (app <- appList) {
              appPuts.add(toAppPut(app))
            }
          }

        }
        sourceTable.put(sourcePuts)
        sourceTable.close()
        appTable.put(appPuts)
        appTable.close()
        sourceConn.close()
        appConn.close()
      }

    })

    /* parseRDD.foreachPartition({ f =>
      import java.sql.{DriverManager,PreparedStatement,Connection}
      var dbconn:Connection = null
      var ps:PreparedStatement = null
      var eps:PreparedStatement = null
      val sql = "insert into HZTP1.F_BRUSH2(zero,clienttype,type_,uid_,time_,phoneid,model_,app,wifimac,guid,verid,ver,flashtype,imei,imei2,defaultconfig,luancherinfo,phonespace,author,oldapplist,incompatible,translate,romid,romtype,m,t,posttime,success_count,error_count,rom_md5,rom_remark,isminiclient,supportflash) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
      val esql = "insert into HZTP1.F_BRUSH2_ERR(filename,data_,msg) values (?,?,?)"
      var temp: (String, Map[String, String])=null
      try {
        dbconn = DriverManager.getConnection(DB_URL,DB_USERNAME,DB_PASSWORD)
        ps = dbconn.prepareStatement(sql)
        eps = dbconn.prepareStatement(esql)
        while (f.hasNext) {
          val result = f.next
          temp = result
          if("1".equals(result._1)){
            val brush = toBrush(result._2)
            ps.setString(1, brush.zero)
						ps.setString(2, brush.clienttype)
						ps.setString(3, brush.type_)
						ps.setString(4, brush.uid)
						ps.setString(5, brush.time)
						ps.setString(6, brush.phoneid)
						ps.setString(7, brush.model)
						val appclob   = oracle.sql.CLOB.createTemporary(dbconn, false,oracle.sql.CLOB.DURATION_SESSION)
            appclob.setString(1, brush.app)
            ps.setClob(8, appclob)
						
						ps.setString(9, brush.wifimac)
						ps.setString(10, brush.guid)
						ps.setString(11, brush.verid)
						ps.setString(12, brush.ver)
						ps.setString(13, brush.flashtype)
						
						val imeiclob   = oracle.sql.CLOB.createTemporary(dbconn, false,oracle.sql.CLOB.DURATION_SESSION)
            imeiclob.setString(1, brush.imei)
            ps.setClob(14, imeiclob)
						
						ps.setString(15, brush.imei2)
						ps.setString(16, brush.defaultconfig)
						ps.setString(17, brush.luancherinfo)
						ps.setString(18, brush.phonespace)
						ps.setString(19, brush.author)
						
						val oldapplistclob   = oracle.sql.CLOB.createTemporary(dbconn, false,oracle.sql.CLOB.DURATION_SESSION)
            oldapplistclob.setString(1, brush.oldapplist)
            ps.setClob(20, oldapplistclob)
						
						ps.setString(21, brush.incompatible)
						val translateclob   = oracle.sql.CLOB.createTemporary(dbconn, false,oracle.sql.CLOB.DURATION_SESSION)
            translateclob.setString(1, brush.translate)
            ps.setClob(22, translateclob)
						ps.setString(23, brush.romid)
						ps.setString(24, brush.romtype)
						ps.setString(25, brush.m)
						ps.setString(26, brush.t)
						ps.setString(27, brush.posttime)
						ps.setString(28, brush.success_count)
						ps.setString(29, brush.error_count)
						ps.setString(30, brush.rom_md5)
						ps.setString(31, brush.rom_remark)
						ps.setString(32, brush.isminiclient)
						ps.setString(33, brush.supportflash)
						ps.addBatch()
          }else{//解析失败
            var filename:String = null
            var param = result._2.get("filename")
            if(!param.isEmpty)
              filename = param.get
            var data:String = null
            param = result._2.get("data")
            if(!param.isEmpty)
              data = param.get
            eps.setString(1, filename)
            val dataclob  = oracle.sql.CLOB.createTemporary(dbconn, false,oracle.sql.CLOB.DURATION_SESSION)
            dataclob.setString(1, data);
						
						
            eps.setClob(2, dataclob);
            eps.setString(3, null)
						eps.addBatch()
          }
         
        }
        
        dbconn.setAutoCommit(false)
				ps.executeBatch()
				eps.executeBatch()
				dbconn.commit()
				dbconn.setAutoCommit(true)
        
      }catch{
        case e:Exception=>
          println("Sql Exception:"+e.getMessage)
          if (!dbconn.isClosed()) { 
            //提交失败，执行回滚操作  
            dbconn.rollback();  
            dbconn.setAutoCommit(true)
          }
          try{
            if(temp!=null){
              eps.clearBatch()
              var filename:String = null
              var param = temp._2.get("filename")
              if(!param.isEmpty)
                filename = param.get
              var data:String = null
              param = temp._2.get("data")
              if(!param.isEmpty)
                data = param.get
              eps.setString(1, filename)
              val clob2   = oracle.sql.CLOB.createTemporary(dbconn, false,oracle.sql.CLOB.DURATION_SESSION)
                clob2.setString(1, data);
              eps.setClob(2, clob2);
              eps.setString(3, e.getMessage)
              eps.execute()
            }
          }catch{
            case e:Exception=>
              println("ERROR Exception:"+e.getMessage)
          }
      }finally{
        if(ps!=null)
          ps.close()
        if(eps!=null)
          eps.close()
        if(dbconn!=null)
          dbconn.close()
      }
    })*/

    sc.stop()
  }

  /**
    * 转化为CpzSource
    * @param imeis
    * @param result
    * @return
    */
  def toCpzSource(imeis: (String, String), result: Map[String, String]): CpzSource = {
    var param: Option[String] = None
    var postdate: String = null
    var operationdate: String = null
    var operationtype: String = null
    var imid: String = null
    var channelid: String = null
    var phonemodel: String = null
    var cooperationtype: String = null
    var cost: String = null
    var ctype: String = null
    var ime1: String = null
    var ime2: String = null
    var currencytype: String = null
    var state: String = null
    var spid: String = null
    var creatorid: String = null
    var adddate: String = null
    var ccount: String = null
    var chcount: String = null
    var simstate: String = null
    var linktype: String = null
    var manufacturid: String = null
    var manufacturname: String = null
    var cdate: String = null
    var phonemodelname: String = null
    var verid: String = null
    var packagelist: String = null
    var cpzsourceid: String = null
    var customid: String = null
    var romid: String = null
    var romtype: String = null
    var packagelist_copy: String = null
    var companyid: String = null
    var systemappcount: String = null
    var userappcount: String = null
    var clienttype: String = null
    var supportflash: String = null
    var acc: String = null

    var brand: String = null
    var scriptmode: String = null
    param = result.get("posttime")
    if (!param.isEmpty)
      postdate = param.get
    param = result.get("time")
    if (!param.isEmpty)
      operationdate = param.get
    param = result.get("operationtype")
    if (!param.isEmpty)
      operationtype = param.get
    imid = null
    channelid = "0"
    param = result.get("phoneid")
    if (!param.isEmpty)
      phonemodel = param.get
    cooperationtype = null
    cost = null
    param = result.get("flashtype")
    if (!param.isEmpty)
      ctype = param.get

    if (StringUtils.isNotBlank(imeis._1))
      ime1 = imeis._1.replace("\r", "") //替换异常字符
    if (StringUtils.isNotBlank(imeis._2))
      ime2 = imeis._2.replace("\r", "") //替换异常字符
    currencytype = null
    state = "1"
    param = result.get("verid")
    if (!param.isEmpty)
      spid = param.get
    param = result.get("uid")
    if (!param.isEmpty)
      creatorid = param.get
    adddate = sdf.format(new Date())
    ccount = "0"
    chcount = "0"
    simstate = null
    linktype = null
    manufacturid = null
    manufacturname = null
    param = result.get("time")
    if (!param.isEmpty)
      cdate = param.get
    param = result.get("model")
    if (!param.isEmpty)
      phonemodelname = param.get
    param = result.get("verid")
    if (!param.isEmpty)
      verid = param.get
    param = result.get("app")
    if (!param.isEmpty)
      packagelist = param.get
    cpzsourceid = UUID.randomUUID().toString().replaceAll("-", "")
    param = result.get("uid")
    if (!param.isEmpty)
      customid = param.get
    param = result.get("romid")
    if (!param.isEmpty)
      romid = param.get
    param = result.get("romtype")
    if (!param.isEmpty)
      romtype = param.get
    packagelist_copy = null
    companyid = null
    systemappcount = null
    userappcount = null
    param = result.get("clienttype")
    if (!param.isEmpty) {
      val v = param.get
      if ("pc".equalsIgnoreCase(v))
        clienttype = "1"
      else if ("box".equalsIgnoreCase(v))
        clienttype = "2"
    }
    param = result.get("supportflash")
    if (!param.isEmpty)
      supportflash = param.get
    param = result.get("Acc")
    if (!param.isEmpty)
      acc = param.get
    brand = result.getOrElse("brand", null)
    scriptmode = result.getOrElse("scriptmode", null)

    val cpzSource = new CpzSource(
      postdate, operationdate, operationtype, imid, channelid, phonemodel, cooperationtype, cost, ctype, ime1, ime2, currencytype, state, spid, creatorid, adddate, ccount, chcount, simstate, linktype, manufacturid, manufacturname, cdate, phonemodelname, verid, packagelist, cpzsourceid, customid, romid, romtype, packagelist_copy, companyid, systemappcount, userappcount, clienttype, supportflash, null)
    cpzSource.acc = acc
    cpzSource.brand = brand
    cpzSource.scriptmode = scriptmode
    cpzSource
  }

  def toSourcePut(result: CpzSource) = {

    // rowkey由两个imei+时间+15位随机数组成
    val key = parseLength(result.ime1) + parseLength(result.ime2) + result.postdate.replace("-", "").replace(":", "").replace(" ", "") + RandomUtil.generateString(15)
    val p = new Put(Bytes.toBytes(key))
    if (StringUtils.isNotBlank(result.postdate))
      p.addColumn(databytes, Bytes.toBytes("postdate"), Bytes.toBytes(result.postdate))
    if (StringUtils.isNotBlank(result.operationdate))
      p.addColumn(databytes, Bytes.toBytes("operationdate"), Bytes.toBytes(result.operationdate))
    if (StringUtils.isNotBlank(result.operationtype))
      p.addColumn(databytes, Bytes.toBytes("operationtype"), Bytes.toBytes(result.operationtype))
    if (StringUtils.isNotBlank(result.imid))
      p.addColumn(databytes, Bytes.toBytes("imid"), Bytes.toBytes(result.imid))
    if (StringUtils.isNotBlank(result.channelid))
      p.addColumn(databytes, Bytes.toBytes("channelid"), Bytes.toBytes(result.channelid))
    if (StringUtils.isNotBlank(result.phonemodel))
      p.addColumn(databytes, Bytes.toBytes("phonemodel"), Bytes.toBytes(result.phonemodel))
    if (StringUtils.isNotBlank(result.cooperationtype))
      p.addColumn(databytes, Bytes.toBytes("cooperationtype"), Bytes.toBytes(result.cooperationtype))
    if (StringUtils.isNotBlank(result.cost))
      p.addColumn(databytes, Bytes.toBytes("cost"), Bytes.toBytes(result.cost))
    if (StringUtils.isNotBlank(result.ctype))
      p.addColumn(databytes, Bytes.toBytes("ctype"), Bytes.toBytes(result.ctype))
    if (StringUtils.isNotBlank(result.ime1))
      p.addColumn(databytes, Bytes.toBytes("ime1"), Bytes.toBytes(result.ime1))
    if (StringUtils.isNotBlank(result.ime2))
      p.addColumn(databytes, Bytes.toBytes("ime2"), Bytes.toBytes(result.ime2))
    if (StringUtils.isNotBlank(result.currencytype))
      p.addColumn(databytes, Bytes.toBytes("currencytype"), Bytes.toBytes(result.currencytype))
    if (StringUtils.isNotBlank(result.state))
      p.addColumn(databytes, Bytes.toBytes("state"), Bytes.toBytes(result.state))
    if (StringUtils.isNotBlank(result.spid))
      p.addColumn(databytes, Bytes.toBytes("spid"), Bytes.toBytes(result.spid))
    if (StringUtils.isNotBlank(result.creatorid))
      p.addColumn(databytes, Bytes.toBytes("creatorid"), Bytes.toBytes(result.creatorid))
    if (StringUtils.isNotBlank(result.adddate))
      p.addColumn(databytes, Bytes.toBytes("adddate"), Bytes.toBytes(result.adddate))
    if (StringUtils.isNotBlank(result.ccount))
      p.addColumn(databytes, Bytes.toBytes("ccount"), Bytes.toBytes(result.ccount))
    if (StringUtils.isNotBlank(result.chcount))
      p.addColumn(databytes, Bytes.toBytes("chcount"), Bytes.toBytes(result.chcount))
    if (StringUtils.isNotBlank(result.simstate))
      p.addColumn(databytes, Bytes.toBytes("simstate"), Bytes.toBytes(result.simstate))
    if (StringUtils.isNotBlank(result.linktype))
      p.addColumn(databytes, Bytes.toBytes("linktype"), Bytes.toBytes(result.linktype))
    if (StringUtils.isNotBlank(result.manufacturid))
      p.addColumn(databytes, Bytes.toBytes("manufacturid"), Bytes.toBytes(result.manufacturid))
    if (StringUtils.isNotBlank(result.manufacturname))
      p.addColumn(databytes, Bytes.toBytes("manufacturname"), Bytes.toBytes(result.manufacturname))
    if (StringUtils.isNotBlank(result.cdate))
      p.addColumn(databytes, Bytes.toBytes("cdate"), Bytes.toBytes(result.cdate))
    if (StringUtils.isNotBlank(result.phonemodelname))
      p.addColumn(databytes, Bytes.toBytes("phonemodelname"), Bytes.toBytes(result.phonemodelname))
    if (StringUtils.isNotBlank(result.verid))
      p.addColumn(databytes, Bytes.toBytes("verid"), Bytes.toBytes(result.verid))
    if (StringUtils.isNotBlank(result.packagelist))
      p.addColumn(databytes, Bytes.toBytes("packagelist"), Bytes.toBytes(result.packagelist))
    if (StringUtils.isNotBlank(result.cpzsourceid))
      p.addColumn(databytes, Bytes.toBytes("cpzsourceid"), Bytes.toBytes(result.cpzsourceid))
    if (StringUtils.isNotBlank(result.customid))
      p.addColumn(databytes, Bytes.toBytes("customid"), Bytes.toBytes(result.customid))
    if (StringUtils.isNotBlank(result.romid))
      p.addColumn(databytes, Bytes.toBytes("romid"), Bytes.toBytes(result.romid))
    if (StringUtils.isNotBlank(result.romtype))
      p.addColumn(databytes, Bytes.toBytes("romtype"), Bytes.toBytes(result.romtype))
    if (StringUtils.isNotBlank(result.packagelist_copy))
      p.addColumn(databytes, Bytes.toBytes("packagelist_copy"), Bytes.toBytes(result.packagelist_copy))
    if (StringUtils.isNotBlank(result.companyid))
      p.addColumn(databytes, Bytes.toBytes("companyid"), Bytes.toBytes(result.companyid))
    if (StringUtils.isNotBlank(result.systemappcount))
      p.addColumn(databytes, Bytes.toBytes("systemappcount"), Bytes.toBytes(result.systemappcount))
    if (StringUtils.isNotBlank(result.userappcount))
      p.addColumn(databytes, Bytes.toBytes("userappcount"), Bytes.toBytes(result.userappcount))
    if (StringUtils.isNotBlank(result.clienttype))
      p.addColumn(databytes, Bytes.toBytes("clienttype"), Bytes.toBytes(result.clienttype))
    if (StringUtils.isNotBlank(result.supportflash))
      p.addColumn(databytes, Bytes.toBytes("supportflash"), Bytes.toBytes(result.supportflash))
    if (StringUtils.isNotBlank(result.acc))
      p.addColumn(databytes, Bytes.toBytes("acc"), Bytes.toBytes(result.acc))
    if (StringUtils.isNotBlank(result.brand))
      p.addColumn(databytes, Bytes.toBytes("brand"), Bytes.toBytes(result.brand))
    if (StringUtils.isNotBlank(result.scriptmode))
      p.addColumn(databytes, Bytes.toBytes("scriptmode"), Bytes.toBytes(result.scriptmode))
    p
  }

  def toAppPut(result: CpzApp) = {
    val key = parseLength(result.ime1) + parseLength(result.ime2) + result.cdate.replace("-", "").replace(":", "").replace(" ", "") + RandomUtil.generateString(15);
    val p = new Put(Bytes.toBytes(key))

    if (StringUtils.isNotBlank(result.appdetailid))
      p.addColumn(databytes, Bytes.toBytes("appdetailid"), Bytes.toBytes(result.appdetailid))
    if (StringUtils.isNotBlank(result.imid))
      p.addColumn(databytes, Bytes.toBytes("imid"), Bytes.toBytes(result.imid))
    if (StringUtils.isNotBlank(result.state))
      p.addColumn(databytes, Bytes.toBytes("state"), Bytes.toBytes(result.state))
    if (StringUtils.isNotBlank(result.adddate))
      p.addColumn(databytes, Bytes.toBytes("adddate"), Bytes.toBytes(result.adddate))
    if (StringUtils.isNotBlank(result.cpzasourceid))
      p.addColumn(databytes, Bytes.toBytes("cpzasourceid"), Bytes.toBytes(result.cpzasourceid))
    if (StringUtils.isNotBlank(result.areatype))
      p.addColumn(databytes, Bytes.toBytes("areatype"), Bytes.toBytes(result.areatype))
    if (StringUtils.isNotBlank(result.cdate))
      p.addColumn(databytes, Bytes.toBytes("cdate"), Bytes.toBytes(result.cdate))
    if (StringUtils.isNotBlank(result.ime1))
      p.addColumn(databytes, Bytes.toBytes("ime1"), Bytes.toBytes(result.ime1))
    if (StringUtils.isNotBlank(result.ime2))
      p.addColumn(databytes, Bytes.toBytes("ime2"), Bytes.toBytes(result.ime2))
    if (StringUtils.isNotBlank(result.pkg))
      p.addColumn(databytes, Bytes.toBytes("pkg"), Bytes.toBytes(result.pkg))
    if (StringUtils.isNotBlank(result.impandsuc))
      p.addColumn(databytes, Bytes.toBytes("impandsuc"), Bytes.toBytes(result.impandsuc))
    p
  }

}