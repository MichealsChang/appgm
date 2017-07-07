package com.lanny.analysistest

import com.lanny.analysistest.util.{HbaseUtil, ImeiUtil}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object SearchArriveListByImei {
  val TABLE_NAME = "sta_v2"
  val COLUMN_FAMILY = "scf"
  val HBASE_ZOOKEEPER_QUORUM = "fenxi-xlg"
  val HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT = "2181"

  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sparkConf = new SparkConf().setAppName("SearchArriveListByImei")
    val sc = new SparkContext(sparkConf)
    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    // 要查询业务数据
    val data = sc.textFile("/test/fenxi/1.csv").map(line => {
      val arr = line.split(",")
      if (arr.length > 4) {
        (arr(4), "") // imei1
      } else {
        null
      }
    }).filter(_!=null)

    val imeisRDD = ImeiUtil.getAllImeis_V2(spark, null, null, null)

    // 保留所有imei
    val imeisFormat = imeisRDD.map(f => {
      val imeiid = f._2
      val imei = f._1
      (imeiid, imei :: Nil)
    }).reduceByKey((a, b) => {
      a ::: b
    })

    val cpzData = data.join(imeisRDD).map(line => {
      val imeiid = line._2._2
      (imeiid, "")
    }).join(imeisFormat).map(line => {
      val imeis = line._2._2
      imeis
    }).mapPartitions(getHBaseInfo)

    cpzData.map(l => l._2._1 + "|" + l._2._2 + "|" + l._1).repartition(1).saveAsTextFile("/test/fenxi/arr")

    sc.stop()
    spark.stop()
  }

  def getHBaseInfo(iter: Iterator[List[String]]): Iterator[(String, (String, String))] = {

    var result = List[(String, (String, String))]()

    val conn = HbaseUtil.getConnection(TABLE_NAME)
    val table = conn.getTable(TableName.valueOf(TABLE_NAME))
    val scan = new Scan()
    scan.setCaching(10000)
    scan.setCacheBlocks(false)
    scan.addColumn(COLUMN_FAMILY.getBytes, "appList".getBytes)

    while (iter.hasNext) {
      val imeis = iter.next()
      var cpzLst = List[(String, (String, String))]()
      for (i <- imeis) {
        scan.setRowPrefixFilter(i.getBytes)
        val resultScanner = table.getScanner(scan)
        val it = resultScanner.iterator()
        if (it.hasNext) {
          val result: Result = it.next()

          val key = Bytes.toString(result.getRow)
          val appList = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "appList".getBytes))
          val accepttime = key.substring(32, 49)
          val year = accepttime.substring(0, 4)
          val month = accepttime.substring(4, 6)
          val day = accepttime.substring(6, 8)
          val hour = accepttime.substring(8, 10)
          val min = accepttime.substring(10, 12)
          val second = accepttime.substring(12, 14)
          val opttime = (year + "-" + month + "-" + day + " " + hour + ":" + min + ":" + second)

          cpzLst.::=(opttime, (key, appList))
        }
      }

      val sortList = cpzLst.sorted
      if (sortList.length > 0) {
        val s = sortList(0)
        result.::=(s)
      }
    }

    table.close()
    conn.close()

    result.iterator
  }
}