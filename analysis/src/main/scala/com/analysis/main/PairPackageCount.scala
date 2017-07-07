package com.analysis.main

import com.analysis.util.ImeiUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * 按月统计到达数据中包名对应IMEI的数目
  *
  * @author 白鑫
  */
object PairPackageCount {

  // args -> 201701
  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("参数异常")
      System.exit(1)
    }

    val year = args(0).substring(0, 4)
    val month = args(0).substring(4, 6)
//    val day = args(0).substring(6, 8)

    //设置序列化器为KryoSerializer,也可以在配置文件中进行配置
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // 超时
    System.setProperty("spark.network.timeout","300s")

    // 设置应用名称，新建Spark环境
    val sparkConf = new SparkConf().setAppName("CountPairPKGImei_" + args(0))
    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
    println("Start " + "CountPairPKGImei_" + args(0))

    import spark.sql

    // 从Hive查询app数据
    sql("use arrival")

    // 从mysql查询imei库数据
    val imeiDF = ImeiUtil.getAllImeis(spark, year, month, null)
    imeiDF.createOrReplaceTempView("imei")

    val sb = new StringBuilder
    sb.append("select distinct app.pkg,imei.imeiid from")
    sb.append("(select pkg, imei")
    sb.append(" from dim_app_old where year=" + year + " and month=" + month)
    sb.append(" union select pkg, imei")
    sb.append(" from dim_app_new where year=" + year + " and month=" + month + ")app")
    sb.append(" inner join imei on app.imei=imei.imei")

    val joinDF = sql(sb.toString())
    joinDF.createOrReplaceTempView("pkgimei")

    // 查询结果imeiid、appid
    val appidImeiDF = sql("select pi.imeiid,br.appid from pkgimei pi inner join base_recommendapp br on br.pkg=pi.pkg")

    val rowRDD = appidImeiDF.rdd.map(r => new Tuple2(r.getString(0), r.getString(1))).reduceByKey((x,y) => x + "|" + y)
      .flatMap(getPairAppid).map(r => (r._2,1)).reduceByKey((x, y) => x+y)
      .map(r => Row(r._1.substring(0,3),r._1.substring(3),r._2.toLong))

    // The schema is encoded in a string
    val schemaString = "appid1 appid2 count"
    // Generate the schema based on the string of schema
    val fields = schemaString.split(" ")
      .map {
        fieldName =>
          if (fieldName.equals("count")) StructField(fieldName, LongType, nullable = true)
          else StructField(fieldName, StringType, nullable = true)
      }
    val schema = StructType(fields)
    // Apply the schema to the RDD
    val countDF = spark.createDataFrame(rowRDD, schema)
    countDF.createOrReplaceTempView("pkgcount")

    sb.clear()
    sb.append("select br1.appname,br1.pkg,br2.appname,br2.pkg,count from pkgcount pc")
    sb.append(" left join base_recommendapp br1 on br1.appid=pc.appid1")
    sb.append(" left join base_recommendapp br2 on br2.appid=pc.appid2")
    sb.append(" order by count desc")
    val resultDF = sql(sb.toString())
    val result = resultDF.rdd.map(r => r.getString(0) + "(" + r.getString(1) + ")" + "\t" + r.getString(2) + "(" + r.getString(3) + ")" + "\t" + r.getLong(4))
    result.saveAsTextFile("/changmi/pairPkgCount/pairPkgCount_" + args(0))

//    val rdd = sql("select firstimei ,secondimei ,opttime  from base_arrival_new where year=2017 and month=04 and day=21 and firstimei='864679033422540'").rdd.map(r => r.getString(0) + "\t" + r.getString(1) + "\t" + r.getTimestamp(2))
    spark.stop()
  }

  def getPairAppid(rdd: Tuple2[String,String]): List[Tuple2[String,String]] = {
    val list = ListBuffer[Tuple2[String,String]]()
    val ids = rdd._2.split("\\|")
    for (i <- 0 until ids.length) {
      for (j <- i+1 until ids.length)
        if (ids(i).toInt < ids(j).toInt) {
          list += Tuple2[String,String](rdd._1,ids(i) + ids(j))
        } else {
          list += Tuple2[String,String](rdd._1,ids(j) + ids(i))
        }
    }
    list.toList
  }
}