package com.analysis.install

import java.net.URLDecoder
import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.text.SimpleDateFormat
import java.util.Date

import com.analysis.util.DbUtil
import org.apache.commons.lang.StringUtils
import org.apache.http.NameValuePair
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.message.BasicNameValuePair
import org.apache.http.util.EntityUtils
import org.apache.spark._

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  * 解析刷机日志，将新的机型入库
  */
object AddPhoneModel {

  //不同环境修改
  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  val UID = "2030"
  val T = "1492760643655"
  val M = "d3cb6b6be6a80b1ce3f1d9772db85d1f"

  def main(args: Array[String]) {

    // 参数格式
    if (args == null || args.length < 1) {
      System.err.println("参数异常")
      System.exit(1)
    }

    // 设置序列化方式
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sparkConf = new SparkConf().setAppName("AddPhoneModel_" + args(0))

    val sc = new SparkContext(sparkConf)

    // 参数：/2017/05/03/12/flashtool_2017050312
    val parseRDD = sc.textFile(args(0)).map(f => {
      try {
        var x = URLDecoder.decode(f, "GBK")
        var map = Map[String, String]()

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
          // 如XX=&YY=c时，对XX= split之后长度为1
          if (t.length == 1)
            map += (t(0) -> "")
          else
            map += (t(0) -> t(1))
        }

        // 机型
        // getOrElse用于当集合或者option中有可能存在空值或不存在要查找的值的情况
        val model = map.getOrElse("model", "").toUpperCase()
        //品牌
        val brand = map.getOrElse("brand", "").toUpperCase()

        val lowModelAndBrand = model + brand
        // 异常数据
        if (StringUtils.isBlank(model) || StringUtils.isBlank(brand) ||
          lowModelAndBrand.contains("ADB") || lowModelAndBrand.contains("DAEMON") ||
          lowModelAndBrand.contains("ERROR") || lowModelAndBrand.contains("WARNING") ||
          lowModelAndBrand.contains("CANNOT")) {
          null
        } else {
          ((model, brand), 1)
        }
      } catch {
        case e: Exception =>
          null
      }
    }).filter(_ != null).distinct()

    // 获取库里的机型品牌信息
    val pres = getPrePhoneModelAndBrand()
    val presRDD = sc.parallelize(pres, 2)
      .map((_, 1))

    val result = parseRDD.subtractByKey(presRDD)
      .map(_._1).collect()

    sc.stop()

    if (result != null) {
      println("new phonemodel size:" + result.size)
      if (result.length > 0) {
        for (v <- result) {
          val map = new java.util.HashMap[String, String]()
          map.put("uid", UID)
          map.put("t", T)
          map.put("m", M)
          map.put("type", "phoneinfo")
          map.put("model", v._1)
          map.put("brand", v._2)
          val res = post("http://tpflash.gaojiquan.org/flashtool/romPackage/searchrom", map)
          println("res:" + res)
        }
      }
    }

    //执行
    val rt = Runtime.getRuntime()
    val cmd = Array("/bin/sh", "-c", "nohup /data/install/spark/bin/spark-submit --master spark://fenxi-xlg:7077 --executor-memory 12G --total-executor-cores 10 --name Data-Analyze --supervise --class com.analysis.test.prd.cpz.ParseBrushFinalSync_V2_3 --jars /data/install/hbase/lib/hbase-client-1.2.2.jar,/data/install/hbase/lib/hbase-server-1.2.2.jar,/data/install/hbase/lib/hbase-common-1.2.2.jar,/data/install/hbase/lib/hbase-protocol-1.2.2.jar,/data/install/hbase/lib/guava-12.0.1.jar,/data/install/hbase/lib/htrace-core-3.1.0-incubating.jar,/data/install/hbase/lib/metrics-core-2.2.0.jar,/home/hadoop/jars/mysql-connector-java-5.1.25.jar,/home/hadoop/jars/fastjson-1.2.1.jar,/home/hadoop/jars/jedis-2.7.2.jar,/home/hadoop/jars/commons-pool2-2.3.jar,/home/hadoop/jars/pinyin4j-2.5.0.jar,/home/hadoop/jars/ojdbc6.jar /home/hadoop/analysis-0.0.1-SNAPSHOT.jar " + args(0) + " > /home/hadoop/spark_hbase.out &")

    try {
      val proc = rt.exec(cmd)
      // 获取进程的标准输入流
      val exitVal = proc.waitFor()
      proc.destroy()
      println("预装任务提交")
    } catch {
      case e: Exception =>
        println(args(0) + "预装任务提交失败：" + e.getMessage)
    }

  }


  def post(url: String, params: java.util.Map[String, String]): String = {
    try {
      val httpPost = new HttpPost(url)
      val client = new DefaultHttpClient()
      val valuePairs = new java.util.ArrayList[NameValuePair](params.size())

      for (entry <- params.entrySet()) {
        val nameValuePair = new BasicNameValuePair(entry.getKey(), String.valueOf(entry.getValue()))
        valuePairs.add(nameValuePair)
      }

      val formEntity = new UrlEncodedFormEntity(valuePairs, "UTF-8")
      httpPost.setEntity(formEntity)
      val resp = client.execute(httpPost)

      val entity = resp.getEntity()
      val respContent = EntityUtils.toString(entity, "UTF-8").trim()
      httpPost.abort()
      client.getConnectionManager().shutdown()

      return respContent
    } catch {
      case e: Exception =>
        e.printStackTrace()
        return null
    }
  }

  //获取初始化基础信息
  def getPrePhoneModelAndBrand(): ArrayBuffer[(String, String)] = {
    var dbconn: Connection = null
    var stmt: Statement = null
    var rs: ResultSet = null

    var list = ArrayBuffer[(String, String)]()
    try {
      dbconn = DriverManager.getConnection(DbUtil.ADMIN_DB_URL, DbUtil.ADMIN_DB_USERNAME, DbUtil.ADMIN_DB_PASSWORD)

      stmt = dbconn.createStatement()

      rs = stmt.executeQuery("select a.phonemodelcode,b.manufacturername from t_phonemodel a left join t_phonemanufactur b on a.manufacturerid = b.manufacturerid group by a.phonemodelcode, b.manufacturername")
      while (rs.next()) {
        var model = rs.getString(1) //转化成大写
        if (StringUtils.isNotBlank(model))
          model = model.toUpperCase()
        var brand = rs.getString(2)
        if (StringUtils.isNotBlank(brand))
          brand = brand.toUpperCase()
        list += ((model, brand))
      }
    } catch {
      case e: Exception =>
        println("获取机型失败:" + e.printStackTrace())
    } finally {
      if (rs != null)
        rs.close()
      if (stmt != null)
        stmt.close()
      if (dbconn != null)
        dbconn.close()
    }
    list
  }


}