package com.analysis.main

import java.text.SimpleDateFormat
import java.util.Date

import com.analysis.util.{DataUtil, MyMultipleTextOutputFormat}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

/**
  *
  * @author 白鑫
  */
object AppLogStreamingOld {

  // 存放解析后的到达数据的路径
  val ARRIVAL_DIR = "/test/old/arrival_data"

  def main(args: Array[String]) {

    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sparkConf = new SparkConf().setAppName("AppLogStreamingOld")

    //每60秒一个批次
    val ssc = new StreamingContext(sparkConf, Seconds(60))

    //从Kafka中读取数据，topic为daplog，该topic包含两个分区
    val kafkaStream = KafkaUtils.createStream(
      ssc,
      "hxf:2181,cfg:2181,jqs:2181,jxf:2181,sxtb:2181", //Kafka集群使用的zookeeper
      "spark", //该消费者使用的group.id
      Map[String, Int]("test" -> 0,"test" -> 1), //日志在Kafka中的topic及其分区
      StorageLevel.MEMORY_AND_DISK_SER).map(_._2)

    kafkaStream.foreachRDD((rdd: RDD[String], time: Time) => {

      val result = rdd.map(log => parseLog(log)).map(genBasicInfo).filter(t =>
        StringUtils.isNotBlank(t._1) && StringUtils.isNotBlank(t._2)
      )
      result.saveAsHadoopFile(ARRIVAL_DIR, classOf[String], classOf[String], classOf[MyMultipleTextOutputFormat[String, String]])
    })



    ssc.start()
    ssc.awaitTermination()

  }

  // 分析日志
  def parseLog(log: String): AppLog = {

    val app = new AppLog
    var data = log
    try {
      var idx = 0
      // 接收服务器获取的IP
      val ip = data.substring(0, log.indexOf("|"))
      data = data.substring(log.indexOf("|") + 1)
      // 13位时间戳,接收服务器的接收的时间
      var tss = data.substring(0, data.indexOf("|"))
      tss = tss.replaceAll("\\.", "")
      val ts = tss.toLong
      // Date postDate = new Date(ts);

      data = data.substring(data.indexOf("|") + 1)

      // 数据来源标识
      val source = data.substring(idx, idx + 2)
      idx += 2

      // 预留位
      val unUsed = data.substring(idx, idx + 3)
      idx += 3

      // 处理方式标识
      val processType = data.substring(idx, idx + 1)
      idx += 1

      data = data.substring(idx)

      if ("1".equals(processType)) {
        app.source = source // 数据来源标识
        app.unUsed = unUsed // 预留，暂时没用到
        app.processType = processType // 处理方式
        app.serverIp = ip
        app.acceptTimestamp = String.valueOf(ts)

        parseBodyForOld(data, app)
        // 设置解析成功标志
        app.parseState = 1
      } else if ("2".equals(processType)) {
        app.source = source // 数据来源标识
        app.unUsed = unUsed // 预留，暂时没用到
        app.processType = processType // 处理方式
        app.serverIp = ip
        app.acceptTimestamp = String.valueOf(ts)
        parseBody(data, app)
        // 设置解析成功标志
        app.parseState = 1
      } else if ("3".equals(processType)) {
        //appList 添加appname
        //com.dsi.ant.server&ANT HAL Service.00000000.ee0dcfb0.0;com.iqoo.secure&i 管家.00000000.c15e53c0.0;com.example.counter_plugin_test&counter_plugin_test.00000000.b0ba37ad.1;
        app.source = source // 数据来源标识
        app.unUsed = unUsed // 预留，暂时没用到
        app.processType = processType // 处理方式
        app.serverIp = ip
        app.acceptTimestamp = String.valueOf(ts)
        parseBody(data, app)
        // 设置解析成功标志
        app.parseState = 1
      } else {
        throw new Exception("缺少六位标识头|" + log)
      }

    } catch {
      case e: Exception =>
        e.printStackTrace()
        app.parseState = 0
        app.rawData = log
    }

    app
  }

  def parseBodyForOld(data: String, app: AppLog): Unit = {

    var idx = 0
    var appdata = data
    // 解压数据
    appdata = appdata.replaceAll("\\*", "\\/")
    appdata = DataUtil.unzip(appdata)
    appdata = DataUtil.decode(appdata)
    // 8位 时间戳 手机操作时间
    val hexTimestamp = appdata.substring(idx, idx + 8)
    idx += 8

    val timestamp = java.lang.Long.parseLong(hexTimestamp, 16)
    val operationDate = new Date(timestamp * 1000 + 946656000000L)
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val operationDateStr = df.format(operationDate)
    app.optTime = operationDateStr
    // 1位 服务商
    val serviceProviderStr = appdata.substring(idx, idx + 1)
    idx += 1

    val serviceProvider = java.lang.Long.parseLong(serviceProviderStr)
    app.serviceProvider = String.valueOf(serviceProvider)
    // 1位 网络类型
    // 0 "未识别";
    // 1 "WIFI";
    // 2 "2G";
    // 3 "3G";
    // 4 "4G";
    val networkTypeStr = appdata.substring(idx, idx + 1)
    idx += 1

    val networkType = Integer.parseInt(networkTypeStr)
    app.netType = String.valueOf(networkType)

    // 1位 IMEI个数
    val imeiCountStr = appdata.substring(idx, idx + 1)
    idx += 1

    val imeiCount = java.lang.Long.parseLong(imeiCountStr, 16)
    app.imeiCount = String.valueOf(imeiCount)

    // 1位 第一个IMEI长度
    val imei1LenHex = appdata.substring(idx, idx + 1)
    idx += 1

    val imei1Len = Integer.parseInt(imei1LenHex, 16)
    // 1位 第二个IMEI长度
    val imei2LenHex = appdata.substring(idx, idx + 1)
    idx += 1

    val imei2Len = Integer.parseInt(imei2LenHex, 16)
    // IMEI1内容
    var imei1 = ""
    if (imei1Len == 14 || imei1Len == 15) {
      imei1 = appdata.substring(idx, idx + imei1Len)
      idx += imei1Len
      app.firstImei = imei1
    }
    // IMEI2内容
    var imei2 = ""
    if (imei2Len == 14 || imei2Len == 15) {
      imei2 = appdata.substring(idx, idx + imei2Len)
      idx += imei2Len
      app.secondImei = imei2
    }

    val channelChoice = appdata.substring(idx, idx + 7)
    if ("tianpai".equals(channelChoice)) {
      // tianpai003
      // 渠道
      val channelIdStr = appdata.substring(idx, idx + 10)
      idx += 10
      app.channelNumber = channelIdStr
    } else {
      // 4位 渠道
      val channelIdStr = appdata.substring(idx, idx + 4)
      idx += 4
      app.channelNumber = channelIdStr
    }
    // 4位 标识
    val biaoshi = appdata.substring(idx, idx + 4)
    idx += 4
    app.flag = biaoshi

    // 2位 手机品牌长度
    val manufacturnameLenStr = appdata.substring(idx, idx + 2)
    idx += 2
    val manufacturnameLen = Integer.parseInt(manufacturnameLenStr, 16)
    // 手机品牌内容
    var manufacturname = appdata.substring(idx, idx + manufacturnameLen)
    idx += manufacturnameLen
    manufacturname = DataUtil.decode(manufacturname)
    app.phoneBrand = manufacturname
    // 2位 手机型号长度
    val phoneModelLenLenStr = appdata.substring(idx, idx + 2)
    idx += 2

    val phoneModelLen = Integer.parseInt(phoneModelLenLenStr, 16)
    // 手机型号内容
    var phoneModel = appdata.substring(idx, idx + phoneModelLen)
    idx += phoneModelLen
    phoneModel = DataUtil.decode(phoneModel)
    app.phoneModel = phoneModel
    // 4位 软件列表长度
    val packageListLenStr = appdata.substring(idx, idx + 4)
    idx += 4
    val packageListLen = Integer.parseInt(packageListLenStr, 16)
    // 软件列表内容
    var packageListStr = appdata.substring(idx, idx + packageListLen)
    idx += packageListLen
    packageListStr = DataUtil.decode(packageListStr)
    app.appList = packageListStr

    if (packageListLen != 51) {
      // 计数器包名长度
      val counterPnameLenHexStr = appdata.substring(idx, idx + 2)
      idx += 2
      val counterPnameLen = Integer.parseInt(counterPnameLenHexStr, 16)
      // 计数器包名
      var counterPname = appdata.substring(idx, idx + counterPnameLen)
      idx += counterPnameLen
      counterPname = DataUtil.decode(counterPname)
      app.counterPkg = counterPname
    }

    // 1位 计数器CRC32长度
    val currentCrc32LenStr = appdata.substring(idx, idx + 1)
    idx += 1
    val currentCrc32Len = Integer.parseInt(currentCrc32LenStr, 16)
    // 当前软件CRC32内容
    val currentCrc32 = appdata.substring(idx, idx + currentCrc32Len)
    idx += currentCrc32Len
    app.counterCrc32 = currentCrc32
    // 1位 经纬度长度
    val geoLenStr = appdata.substring(idx, idx + 1)
    idx += 1
    val geoLen = Integer.parseInt(geoLenStr, 16)
    // 经纬度内容
    val geoStr = appdata.substring(idx, idx + geoLen)
    idx += geoLen
    val geo = java.lang.Long.parseLong(geoStr, 16)
    app.longitude = String.valueOf(DataUtil.getLongitude(geo))
    app.latitude = String.valueOf(DataUtil.getLatitude(geo))
    // 手机获取的IP地址
    val ip2LenStr = appdata.substring(idx, idx + 1)
    idx += 1
    val ip2Len = Integer.parseInt(ip2LenStr, 16)
    var ip2 = appdata.substring(idx, idx + ip2Len)
    idx += ip2Len
    ip2 = decodeIP(ip2)

    app.phoneIp = ip2

    // 1位 SIM卡状态 0：无效，1：有效
    val simStateStr = appdata.substring(idx, idx + 1)
    idx += 1
    // int simState = Integer.parseInt(simStateStr);
    app.simStatus = simStateStr
    // 手机到达次数
    val serialnumber = appdata.substring(idx)
    app.arriveTimes = serialnumber

  }

  def parseBody(data: String, app: AppLog): Unit = {
    var idx = 0
    var appdata = data
    // 解压数据
    appdata = appdata.replaceAll("\\*", "\\/")
    appdata = DataUtil.unzip(appdata)
    appdata = DataUtil.decode(appdata)
    // 8位 时间戳 手机操作时间
    val hexTimestamp = appdata.substring(idx, idx + 8)
    idx += 8

    val timestamp = java.lang.Long.parseLong(hexTimestamp, 16)
    val operationDate = new Date(timestamp * 1000 + 946656000000L)
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val operationDateStr = df.format(operationDate)
    app.optTime = operationDateStr
    // 1位 服务商
    val serviceProviderStr = appdata.substring(idx, idx + 1)
    idx += 1

    val serviceProvider = java.lang.Long.parseLong(serviceProviderStr)
    app.serviceProvider = String.valueOf(serviceProvider)
    // 1位 网络类型
    // 0 "未识别";
    // 1 "WIFI";
    // 2 "2G";
    // 3 "3G";
    // 4 "4G";
    val networkTypeStr = appdata.substring(idx, idx + 1)
    idx += 1

    val networkType = Integer.parseInt(networkTypeStr)
    app.netType = String.valueOf(networkType)

    // 1位 IMEI个数
    val imeiCountStr = appdata.substring(idx, idx + 1)
    idx += 1

    val imeiCount = java.lang.Long.parseLong(imeiCountStr, 16)
    app.imeiCount = String.valueOf(imeiCount)

    // 1位 第一个IMEI长度
    val imei1LenHex = appdata.substring(idx, idx + 1)
    idx += 1

    val imei1Len = Integer.parseInt(imei1LenHex, 16)
    // 1位 第二个IMEI长度
    val imei2LenHex = appdata.substring(idx, idx + 1)
    idx += 1

    val imei2Len = Integer.parseInt(imei2LenHex, 16)
    // 1位 第三个IMEI长度
    var imei3Len = 0
    if (imeiCount == 3) {
      val imei3LenHex = appdata.substring(idx, idx + 1)
      idx += 1
      imei3Len = Integer.parseInt(imei3LenHex, 16)
    }
    // IMEI1内容
    var imei1 = ""
    if (imei1Len == 14 || imei1Len == 15) {
      imei1 = appdata.substring(idx, idx + imei1Len)
      idx += imei1Len
      app.firstImei = imei1
    }
    // IMEI2内容
    var imei2 = ""
    if (imei2Len == 14 || imei2Len == 15) {
      imei2 = appdata.substring(idx, idx + imei2Len)
      idx += imei2Len
      app.secondImei = imei2
    }
    // IMEI3内容
    var imei3 = ""
    if (imei3Len > 0) {
      if (imei3Len == 14 || imei3Len == 15) {
        imei3 = appdata.substring(idx, idx + imei3Len)
        idx += imei3Len
        app.thirdImei = imei3
      }
    }

    val channelChoice = appdata.substring(idx, idx + 7)
    if ("tianpai".equals(channelChoice)) {
      // tianpai003
      // 渠道
      val channelIdStr = appdata.substring(idx, idx + 10)
      idx += 10
      app.channelNumber = channelIdStr
    } else {
      // 4位 渠道
      val channelIdStr = appdata.substring(idx, idx + 4)
      idx += 4
      app.channelNumber = channelIdStr
    }
    // 4位 标识
    val biaoshi = appdata.substring(idx, idx + 4)
    idx += 4
    app.flag = biaoshi

    // 2位 手机品牌长度
    val manufacturnameLenStr = appdata.substring(idx, idx + 2)
    idx += 2
    val manufacturnameLen = Integer.parseInt(manufacturnameLenStr, 16)
    // 手机品牌内容
    var manufacturname = appdata.substring(idx, idx + manufacturnameLen)
    idx += manufacturnameLen
    manufacturname = DataUtil.decode(manufacturname)
    app.phoneBrand = manufacturname
    // 2位 手机型号长度
    val phoneModelLenLenStr = appdata.substring(idx, idx + 2)
    idx += 2

    val phoneModelLen = Integer.parseInt(phoneModelLenLenStr, 16)
    // 手机型号内容
    var phoneModel = appdata.substring(idx, idx + phoneModelLen)
    idx += phoneModelLen
    phoneModel = DataUtil.decode(phoneModel)
    app.phoneModel = phoneModel
    // 4位 软件列表长度
    val packageListLenStr = appdata.substring(idx, idx + 4)
    idx += 4
    val packageListLen = Integer.parseInt(packageListLenStr, 16)
    // 软件列表内容
    var packageListStr = appdata.substring(idx, idx + packageListLen)
    idx += packageListLen
    packageListStr = DataUtil.decode(packageListStr)
    app.appList = packageListStr

    // 计数器包名长度
    val counterPnameLenHexStr = appdata.substring(idx, idx + 2)
    idx += 2
    val counterPnameLen = Integer.parseInt(counterPnameLenHexStr, 16)
    // 计数器包名
    var counterPname = appdata.substring(idx, idx + counterPnameLen)
    idx += counterPnameLen
    counterPname = DataUtil.decode(counterPname)
    app.counterPkg = counterPname

    // 1位 计数器CRC32长度
    val currentCrc32LenStr = appdata.substring(idx, idx + 1)
    idx += 1
    val currentCrc32Len = Integer.parseInt(currentCrc32LenStr, 16)
    // 当前软件CRC32内容
    val currentCrc32 = appdata.substring(idx, idx + currentCrc32Len)
    idx += currentCrc32Len
    app.counterCrc32 = currentCrc32
    // 1位 经纬度长度
    val geoLenStr = appdata.substring(idx, idx + 1)
    idx += 1
    val geoLen = Integer.parseInt(geoLenStr, 16)
    // 经纬度内容
    val geoStr = appdata.substring(idx, idx + geoLen)
    idx += geoLen
    val geo = java.lang.Long.parseLong(geoStr, 16)
    app.longitude = String.valueOf(DataUtil.getLongitude(geo))
    app.latitude = String.valueOf(DataUtil.getLatitude(geo))
    // 手机获取的IP地址
    val ip2LenStr = appdata.substring(idx, idx + 1)
    idx += 1
    val ip2Len = Integer.parseInt(ip2LenStr, 16)
    var ip2 = appdata.substring(idx, idx + ip2Len)
    idx += ip2Len
    ip2 = decodeIP(ip2)

    app.phoneIp = ip2

    // 1位 SIM卡状态 0：无效，1：有效
    val simStateStr = appdata.substring(idx, idx + 1)
    idx += 1
    // int simState = Integer.parseInt(simStateStr);
    app.simStatus = simStateStr

    // sim卡序列号
    val simSNLenStr = appdata.substring(idx, idx + 1)
    idx += 1
    val simSNLen = Integer.parseInt(simSNLenStr, 16)
    val simSN = appdata.substring(idx, idx + simSNLen)
    idx += simSNLen
    app.simSequence = simSN

    // 手机的唯一标识(自己实现的)
    val phoneIdLenStr = appdata.substring(idx, idx + 2)
    idx += 2
    val phoneIdLen = Integer.parseInt(phoneIdLenStr, 16)
    val phoneId = appdata.substring(idx, idx + phoneIdLen)
    idx += phoneIdLen
    app.identity = phoneId

    // 手机到达次数
    val serialnumber = appdata.substring(idx)
    app.arriveTimes = serialnumber
  }

  def decodeIP(ipHexStr: String): String = {
    if (StringUtils.isBlank(ipHexStr)) {
      StringUtils.EMPTY
    } else {
      val ipaddress = java.lang.Long.parseLong(ipHexStr, 16)
      val sb = new StringBuffer("")
      sb.append(String.valueOf((ipaddress >>> 24)))
      sb.append(".")
      sb.append(String.valueOf((ipaddress & 0x00FFFFFF) >>> 16))
      sb.append(".")
      sb.append(String.valueOf((ipaddress & 0x0000FFFF) >>> 8))
      sb.append(".")
      sb.append(String.valueOf((ipaddress & 0x000000FF)))
      sb.toString()
    }

  }

  def genBasicInfo(app: AppLog): Tuple2[String, String] = {

    var day = ""
    val result = new StringBuilder
    val sb = new StringBuilder()
    try {

      if (app != null && app.parseState == 1) {

        /*					if (!("BBBB".equals(sitem.getChannelNumber()) || sitem.getChannelNumber().endsWith("0")
                      || "A0".equals(sitem.getSource())
                  // 对接天湃端数据，先过滤掉这部分数据
                  // 2016-12-01 放开tianpai过滤
                  // || sitem.getChannelNumber().startsWith("tianpai")
                  )) {}*/


        val date = new Date(app.acceptTimestamp.toLong)
                val df = new SimpleDateFormat("yyyy/MM/dd/")
        day = df.format(date)

        val processType = app.processType

        val appList = app.appList
        // val simStatus =
        // Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes,
        // "simStatus".getBytes))
        val arr = appList.split(";")
        // var pkg_scrcs = new StringBuilder()

        // 获取签名crc和包名
        try {
          for (i <- 0 until arr.length) {
            val eme = arr(i)
            if ("1".equals(processType)) {
              // 老版本中只回传包名_apk文件crc
              // 格式“com.qihoo.appstore.FE1E4867”
              val idx = eme.lastIndexOf(".")
              val pkg = eme.substring(0, idx)
              val scrc = checkCrc(eme.substring(idx + 1))
              sb.append(";" + scrc + "," + pkg)

            } else if ("2".equals(processType)) {
              // processType == "2"
              // 新版本回传包名_apk文件crc.签名文件crc.安装分区
              // com.qihoo.appstore.FE1E4867
              // 或com.zj_lab.fireworks3.baidu_D60FA026.57113dce.1
              val lastIndex = eme.lastIndexOf(".")
              val last = eme.substring(lastIndex + 1)
              if (last.length() == 1) {
                // com.zj_lab.fireworks3.baidu_D60FA026.57113dce.1
                val pkg = eme.substring(0, eme.lastIndexOf("_"))
                val scrc = checkCrc(eme.substring(
                  eme.lastIndexOf("_") + 1).split("\\.")(1))
                sb.append(";" + scrc + "," + pkg)
              } else {
                // com.qihoo.appstore.FE1E4867
                val pkg = eme.substring(0, lastIndex)
                val scrc = checkCrc(eme.substring(lastIndex + 1))
                sb.append(";" + scrc + "," + pkg)
              }
            } else if ("3".equals(processType)) {
              //com.dsi.ant.server&ANT HAL Service.00000000.ee0dcfb0.0;com.iqoo.secure&i 管家.00000000.c15e53c0.0;com.example.counter_plugin_test&counter_plugin_test.00000000.b0ba37ad.1;
              //int lastIndex = eme.lastIndexOf(".");
              //String last = eme.substring(lastIndex + 1);
              val pkg = eme.substring(0, eme.indexOf("&"))
              val ex_pkg = eme.substring(eme.indexOf("&") + 1) //ANT HAL Service.00000000.ee0dcfb0.0
              var str = ex_pkg.substring(0, ex_pkg.lastIndexOf(".")) //ANT HAL Service.00000000.ee0dcfb0
              val scrc = checkCrc(str.substring(str.lastIndexOf(".") + 1))
              str = str.substring(0, str.lastIndexOf(".")) //ANT HAL Service.00000000
              val appname = str.substring(0, str.lastIndexOf(".")) //ANT HAL Service
              sb.append(";" + scrc + "," + pkg + "," + appname)
            } else {
              val lastIndex = eme.lastIndexOf(".")
              // com.qihoo.appstore.FE1E4867
              val pkg = eme.substring(0, lastIndex)
              val scrc = checkCrc(eme.substring(lastIndex + 1))
              sb.append(";" + scrc + "," + pkg)
            }
          }
        } catch {
          case e: Exception =>
            println("解析签名crc和包名出错")
            e.printStackTrace()
        }
        var scrc_pkgs = ""
        if (sb.length > 0)
          scrc_pkgs = sb.substring(1)
        // D0|2|119.48.50.222|2016-10-31 12:47:58|0|1|869633027675964| | |D001|D001|Xiaomi|Redmi Note 2|d7a71299,com.wandoujia.phoenix2;36c205b4,com.funshion.video.player;fe6c660e,com.mtk.telephony|com.huohou.apps|b6737115|125.298329|43.86323|0| |66761558fd1679ab9733b6250a36a1d1|0|1477889123637
        result.append(if (StringUtils.isNotBlank(app.source)) app.source else " ")
        result.append("|")
        result.append(if (StringUtils.isNotBlank(app.processType)) app.processType else " ")
        result.append("|")
        result.append(if (StringUtils.isNotBlank(app.serverIp)) app.serverIp else " ")
        result.append("|")
        result.append(if (StringUtils.isNotBlank(app.optTime)) app.optTime else " ")
        result.append("|")
        result.append(if (StringUtils.isNotBlank(app.serviceProvider)) app.serviceProvider else " ")
        result.append("|")
        result.append(if (StringUtils.isNotBlank(app.netType)) app.netType else " ")
        result.append("|")
        result.append(if (StringUtils.isNotBlank(app.firstImei)) app.firstImei else " ")
        result.append("|")
        result.append(if (StringUtils.isNotBlank(app.secondImei)) app.secondImei else " ")
        result.append("|")
        result.append(if (StringUtils.isNotBlank(app.thirdImei)) app.thirdImei else " ")
        result.append("|")
        result.append(if (StringUtils.isNotBlank(app.channelNumber)) app.channelNumber else " ")
        result.append("|")
        result.append(if (StringUtils.isNotBlank(app.flag)) app.flag else " ")
        result.append("|")
        result.append(if (StringUtils.isNotBlank(app.phoneBrand)) app.phoneBrand else " ")
        result.append("|")
        result.append(if (StringUtils.isNotBlank(app.phoneModel)) app.phoneModel else " ")
        result.append("|")
        result.append(if (StringUtils.isNotBlank(scrc_pkgs)) scrc_pkgs else " ")
        result.append("|")
        result.append(if (StringUtils.isNotBlank(app.counterPkg)) app.counterPkg else " ")
        result.append("|")
        result.append(if (StringUtils.isNotBlank(app.counterCrc32)) app.counterCrc32 else " ")
        result.append("|")
        result.append(if (StringUtils.isNotBlank(app.longitude)) app.longitude else " ")
        result.append("|")
        result.append(if (StringUtils.isNotBlank(app.latitude)) app.latitude else " ")
        result.append("|")
        result.append(if (StringUtils.isNotBlank(app.simStatus)) app.simStatus else " ")
        result.append("|")
        result.append(if (StringUtils.isNotBlank(app.simSequence)) app.simSequence else " ")
        result.append("|")
        result.append(if (StringUtils.isNotBlank(app.identity)) app.identity else " ")
        result.append("|")
        result.append(if (StringUtils.isNotBlank(app.arriveTimes)) app.arriveTimes else " ")
        result.append("|")
        result.append(if (StringUtils.isNotBlank(app.acceptTimestamp)) app.acceptTimestamp else " ")


      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    new Tuple2[String, String](day, result.toString())

  }

  def checkCrc(crc: String): String = {
    var crc32 = crc
    if (crc.length() < 8)
    // 补零
      crc32 = crc.format("%1$0" + (8 - crc.length()) + "d", new Integer(0)) + crc
    crc32
  }
}
