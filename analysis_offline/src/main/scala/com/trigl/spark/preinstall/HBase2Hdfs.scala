package com.trigl.spark.preinstall

import com.trigl.spark.utils.{HBaseUtil, MyMultipleTextOutputFormat}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 从一个集群的HBase获取数据传到另一个集群的HDFS
  * created by Trigl at 2017-05-27 11:10
  */
object HBase2Hdfs {

  // 存放解析后的预装数据的路径
  val CPZ_DIR = "/changmi/cpz_data"

  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // 解决各种超时问题
    System.setProperty("spark.network.timeout", "600s")
    val sparkConf = new SparkConf().setAppName("HBase2Hdfs_cpz")
    val sc = new SparkContext(sparkConf)

    // 创建HBase configuration
    val hBaseConf = HBaseConfiguration.create()
    hBaseConf.set("hbase.zookeeper.quorum", HBaseUtil.HBASE_ZOOKEEPER_QUORUM)
    hBaseConf.set("hbase.zookeeper.property.clientPort", HBaseUtil.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT)
    hBaseConf.set(TableInputFormat.INPUT_TABLE, HBaseUtil.TABLE_NAME_CPZ_SOURCE_V2)
    hBaseConf.setInt("hbase.rpc.timeout", 1200000)
    hBaseConf.setInt("hbase.client.operation.timeout", 1200000)
    hBaseConf.setInt("hbase.client.scanner.timeout.period", 1200000)

    // 应用newAPIHadoopRDD读取HBase，返回NewHadoopRDD
    val hbaseRDD = sc.newAPIHadoopRDD(hBaseConf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    // 将数据映射为表，也就是将RDD转化为dataframe schema
    val resRDD = hbaseRDD.map(tuple => tuple._2)
      .map(genBasicInfo).filter(t =>
      StringUtils.isNotBlank(t._1) && StringUtils.isNotBlank(t._2)
    )

    resRDD.repartition(1).saveAsHadoopFile(CPZ_DIR, classOf[String], classOf[String], classOf[MyMultipleTextOutputFormat[String, String]])

    sc.stop()
  }

  /**
    * 解析HBase获取的表
    *
    * @param row
    * @return
    */
  def genBasicInfo(row: Result): (String, String) = {
    var day = ""
    val result = new StringBuilder
    try {
      // 解析所有字段
      var ime1 = Bytes.toString(row.getValue(Bytes.toBytes(HBaseUtil.COLUMN_FAMILY), Bytes.toBytes("ime1")))
      var ime2 = Bytes.toString(row.getValue(Bytes.toBytes(HBaseUtil.COLUMN_FAMILY), Bytes.toBytes("ime2")))
      ime1 = if(StringUtils.isNotBlank(ime1)) ime1.replaceAll("(?:\\\\r|\\\\x|/)", "") else " "
      ime2 = if(StringUtils.isNotBlank(ime2)) ime2.replaceAll("(?:\\\\r|\\\\x|/)", "") else " "
      // 替换特殊字符，合并imei
      val imeis = ime1 + ";" + ime2
      val customid = Bytes.toString(row.getValue(Bytes.toBytes(HBaseUtil.COLUMN_FAMILY), Bytes.toBytes("customid")))
      val phonemodelid = Bytes.toString(row.getValue(Bytes.toBytes(HBaseUtil.COLUMN_FAMILY), Bytes.toBytes("phonemodel")))
      val phonemodelname = Bytes.toString(row.getValue(Bytes.toBytes(HBaseUtil.COLUMN_FAMILY), Bytes.toBytes("phonemodelname")))
      val brand = Bytes.toString(row.getValue(Bytes.toBytes(HBaseUtil.COLUMN_FAMILY), Bytes.toBytes("brand")))
      val ctype = Bytes.toString(row.getValue(Bytes.toBytes(HBaseUtil.COLUMN_FAMILY), Bytes.toBytes("ctype")))
      val spid = Bytes.toString(row.getValue(Bytes.toBytes(HBaseUtil.COLUMN_FAMILY), Bytes.toBytes("spid")))
      val cdate = Bytes.toString(row.getValue(Bytes.toBytes(HBaseUtil.COLUMN_FAMILY), Bytes.toBytes("cdate")))
      var postdata = Bytes.toString(row.getValue(Bytes.toBytes(HBaseUtil.COLUMN_FAMILY), Bytes.toBytes("postdata")))
      val clienttype = Bytes.toString(row.getValue(Bytes.toBytes(HBaseUtil.COLUMN_FAMILY), Bytes.toBytes("clienttype")))
      val systemappcount = Bytes.toString(row.getValue(Bytes.toBytes(HBaseUtil.COLUMN_FAMILY), Bytes.toBytes("systemappcount")))
      val userappcountuserappcount = Bytes.toString(row.getValue(Bytes.toBytes(HBaseUtil.COLUMN_FAMILY), Bytes.toBytes("systemappcount")))
      val acc = Bytes.toString(row.getValue(Bytes.toBytes(HBaseUtil.COLUMN_FAMILY), Bytes.toBytes("acc")))
      val scriptmode = Bytes.toString(row.getValue(Bytes.toBytes(HBaseUtil.COLUMN_FAMILY), Bytes.toBytes("scriptmode")))
      val applist = Bytes.toString(row.getValue(Bytes.toBytes(HBaseUtil.COLUMN_FAMILY), Bytes.toBytes("packagelist")))

      // 如果服务器时间为空就使用客户端时间
      if (StringUtils.isBlank(postdata)) {
        postdata = cdate
      }
      day = postdata.substring(0, 4) + "/" + postdata.substring(5, 7) + "/" + postdata.substring(8, 10) + "/"

      if (StringUtils.isNotBlank(imeis)) {
        result.append(imeis)
        result.append("|")
        result.append(if (StringUtils.isNotBlank(customid)) customid else 0)
        result.append("|")
        result.append(if (StringUtils.isNotBlank(phonemodelid)) phonemodelid else 0)
        result.append("|")
        result.append(if (StringUtils.isNotBlank(phonemodelname)) phonemodelname else " ")
        result.append("|")
        result.append(if (StringUtils.isNotBlank(brand)) brand else " ")
        result.append("|")
        result.append(if (StringUtils.isNotBlank(ctype)) ctype else 0)
        result.append("|")
        result.append(if (StringUtils.isNotBlank(spid)) spid else 0)
        result.append("|")
        result.append(if (StringUtils.isNotBlank(cdate)) cdate else " ")
        result.append("|")
        result.append(if (StringUtils.isNotBlank(postdata)) postdata else " ")
        result.append("|")
        result.append(if (StringUtils.isNotBlank(clienttype)) clienttype else 0)
        result.append("|")
        result.append(if (StringUtils.isNotBlank(systemappcount)) systemappcount else 0)
        result.append("|")
        result.append(if (StringUtils.isNotBlank(userappcountuserappcount)) userappcountuserappcount else 0)
        result.append("|")
        result.append(if (StringUtils.isNotBlank(acc)) acc else " ")
        result.append("|")
        result.append(if (StringUtils.isNotBlank(scriptmode)) scriptmode else 0)
        result.append("|")
        result.append(if (StringUtils.isNotBlank(applist)) applist else " ")
      }

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    (day, result.toString())

  }
}