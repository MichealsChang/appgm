package com.analysis.main

import com.analysis.util.ImeiUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * 按月统计到达数据中包名对应IMEI的数目
  *
  * @author 白鑫
  */
object PackageCount {

  // args -> 201701
  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("参数异常")
      System.exit(1)
    }

    val year = args(0).substring(0, 4)
    val month = args(0).substring(4, 6)
    //val day = args(0).substring(6, 8)

    //设置序列化器为KryoSerializer,也可以在配置文件中进行配置
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // 超时
    System.setProperty("spark.network.timeout","300s")

    // 设置应用名称，新建Spark环境
    val sparkConf = new SparkConf().setAppName("CountPKGImei_" + args(0))
    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
    println("Start " + "CountPKGImei_" + args(0))

    import spark.sql

    // 从Hive查询app数据
    sql("use arrival")

    // 从mysql查询imei数据
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

    val countSql = "select pkg,count(pkg) pkgnum from pkgimei group by pkg order by pkgnum desc"
    val countDF = sql(countSql)
    val result = countDF.rdd.map(r => r.getString(0) + "\t" + r.getLong(1))
    // 统计结果存入文件中
    result.saveAsTextFile("/changmi/pkgCount/pkgCount_" + args(0))

    spark.stop()
  }
}