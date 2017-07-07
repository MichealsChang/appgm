package com.lanny.analysistest

import com.lanny.analysistest.util.{DbUtil, ImeiUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.SparkSession

/**
 * 将刷机数据导入到数据库 ，新版本
 */
object ResultToDB {
  //不同环境修改
  val RESULT_APP_TABLENAME="t_recommend_indiv"
  val RESULT_FOLDER_TABLENAME="t_recommend_folder"
  val RESULT_HOME_TABLENAME="t_recommend_home"
  val HDFS_RECOMMEND_APP = "/changmi/recommend/result/app/"
  val HDFS_RECOMMEND_FOLDER = "/changmi/recommend/result/folder/"
  val HDFS_RECOMMEND_HOME = "/changmi/recommend/result/home/"

  def main(args: Array[String]) {
    // 20170418
    if (args == null || args.length < 1) {
      System.err.println("参数异常")
      System.exit(1)
    }

    val year = args(0).substring(0, 4)
    val month = args(0).substring(4, 6)
    val day = args(0).substring(6, 8)
    val rmdType = args(1) // 推荐类型:app,folder

    //System.setProperty("spark.scheduler.mode", "FAIR")
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sparkConf = new SparkConf().setAppName("ResultToDB_" + args(1) + "_" + args(0))
    val sc = new SparkContext(sparkConf)
    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    import spark.sql
    sql("use arrival")

    // 通过JDBC查询所有imei匹配
    val imeisRDD = ImeiUtil.getAllImeis_V2(spark, year, month, day)

    // 保留所有imei
    val imeisFormat = imeisRDD.map(f => {
      val imeiid = f._2
      val imei = f._1
      (imeiid, imei :: Nil)
    }).reduceByKey((a, b) => {
      a ::: b
    })

    val recommender = new AppRecommend(spark)

    if ("app".equals(rmdType)) {
      val result = sc.textFile(HDFS_RECOMMEND_APP + year + "/" + month + "/" + day)
        .map(r => {
          val arr = r.split("\\|")
          (arr(0), arr(1))
        })

      recommender.resultToDB(spark, result, imeisFormat, RESULT_APP_TABLENAME, null)
    }

    if ("folder".equals(rmdType)) {
      val resultDir = HDFS_RECOMMEND_FOLDER + year + "/" + month + "/" + day + "/"
      val rmd_201 = recommender.recommend(resultDir, imeisRDD, "201")
      val rmd_202 = recommender.recommend(resultDir, imeisRDD, "202")
      val rmd_203 = recommender.recommend(resultDir, imeisRDD, "203")
      val rmd_204 = recommender.recommend(resultDir, imeisRDD, "204")
      val rmd_205 = recommender.recommend(resultDir, imeisRDD, "205")
      val rmd_206 = recommender.recommend(resultDir, imeisRDD, "206")

      // 结果组合在一起，可以一次性入Mysql，不用分多次入库，且减少记录数
      val result = rmd_201.leftOuterJoin(rmd_202).map(r=>{
        val imei = r._1
        val re_201 = r._2._1
        val re_202 = r._2._2.orNull
        (imei,(re_201,re_202))
      }).leftOuterJoin(rmd_203).map(r=>{
        val imei = r._1
        val re_201 = r._2._1._1
        val re_202 = r._2._1._2
        val re_203 = r._2._2.orNull
        (imei,(re_201,re_202,re_203))
      }).leftOuterJoin(rmd_204).map(r => {
        val imei = r._1
        val re_201 = r._2._1._1
        val re_202 = r._2._1._2
        val re_203 = r._2._1._3
        val re_204 = r._2._2.orNull
        (imei,(re_201,re_202,re_203,re_204))
      }).leftOuterJoin(rmd_205).map(r => {
        val imei = r._1
        val re_201 = r._2._1._1
        val re_202 = r._2._1._2
        val re_203 = r._2._1._3
        val re_204 = r._2._1._4
        val re_205 = r._2._2.orNull
        (imei,(re_201,re_202,re_203,re_204,re_205))
      }).leftOuterJoin(rmd_206).map(r=>{
        val imei = r._1
        val re_201 = r._2._1._1
        val re_202 = r._2._1._2
        val re_203 = r._2._1._3
        val re_204 = r._2._1._4
        val re_205 = r._2._1._5
        val re_206 = r._2._2.orNull
        (imei,(re_201,re_202,re_203,re_204,re_205,re_206))
      })

      recommender.folderResultToDB(spark, result, imeisFormat, RESULT_FOLDER_TABLENAME)
    }

    if ("item-based".equals(rmdType)) {
      val result = sc.textFile(HDFS_RECOMMEND_HOME)
        .map(r => {
          val arr = r.split("\\|")
          (arr(0), arr(1))
        })

      recommender.resultToDB(spark, result, imeisFormat, RESULT_HOME_TABLENAME, null)
    }

    
//    recommend.foreachPartition(f=>{
//      val jedis = RedisClient.pool.getResource
//      val tx = jedis.multi()
//      for(r<-f){
//        val imei = r._1
//        //appname,packagename,downloadurl,cversion,icon,desc,updatetime
//        val appname = r._2._1
//        val packagename = r._2._2
//        val downloadurl = r._2._3
//        val cversion = r._2._4
//        val icon = r._2._5
//        val desc = r._2._6
//        var map = Map[String,String]();
//				map+=("appname"->appname)
//				map+=("appurl"->downloadurl)
//				map+=("vernum"->cversion)
//				map+=("pkgname"->packagename)
//				map+=("appimg"->icon)
//				map+=("appdesc"->desc)
//		    tx.hmset("recommend:"+imei, map.asJava)
//			  tx.exec()
//      }
//      jedis.close()
//    })

    sc.stop()
    spark.stop()
  }


}