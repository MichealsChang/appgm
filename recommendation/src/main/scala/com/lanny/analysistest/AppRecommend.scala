package com.lanny.analysistest

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSONArray, JSONObject}
import com.lanny.analysistest.util.{DbUtil, EncodeUtil}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SaveMode, _}

import scala.collection.mutable.ArrayBuffer

/**
  *
  * created by Trigl at 2017-05-06 13:25
  */
class AppRecommend(private val spark: SparkSession) {

  import spark.implicits._
  import spark.sql

  def buildUserMapping(rawUserAppData: DataFrame): DataFrame = {
    rawUserAppData.select("user").distinct().rdd.map(f => {
      val obj = f(0).toString()
      (obj, obj.hashCode())
    }).toDF("user", "userid")
  }

  def buildAppMapping(rawUserAppData: DataFrame): DataFrame = {
    rawUserAppData.select("app").distinct().rdd.map(f => {
      val obj = f(0).toString()
      (obj, obj.hashCode())
    }).toDF("app", "appid")
  }

  /**
    * 推荐top howmany个app
    *
    * @param model
    * @param userID
    * @param howMany
    * @return
    */
  def makeRecommendations(model: ALSModel, userID: Int, howMany: Int, exculde: DataFrame): DataFrame = {
    val toRecommend = model.itemFactors
      .select($"id".as("appid"))
      .except(exculde) //去除已有的
      .withColumn("userid", lit(userID))
    model.transform(toRecommend)
      .select("userid", "appid", "prediction")
      .orderBy($"prediction".desc)
      .limit(howMany)
  }

  /**
    * 得出推荐结果
    *
    * @param rawUserAppData
    */
  def calcPrediction(rawUserAppData: DataFrame, recommendAppList: DataFrame): RDD[Row] = {
    //初始化样本数据，转化为user:Stirng,app:Stirng,rating:Double
    //    val trainData = rawUserAppData
    //构造String id 到整形id的映射(基于hashcode)
    val userMapping = buildUserMapping(rawUserAppData)
    val appMapping = buildAppMapping(rawUserAppData)
    userMapping.createOrReplaceTempView("userMapping")
    appMapping.createOrReplaceTempView("appMapping")
    rawUserAppData.createOrReplaceTempView("trainData")
    //转为userid:Int,appid:Int,rating:Double格式的数据
    val trainData_format = sql("select b.userid,c.appid,a.rating from trainData a left join userMapping b on a.user = b.user left join appMapping c on a.app = c.app")
    //初始化ALS模块，设置参数
    val model = new ALS().setRank(12).setMaxIter(10).setRegParam(0.2).setUserCol("userid").setItemCol("appid").setRatingCol("rating").setPredictionCol("prediction") //预测列
      .fit(trainData_format)
    //topN阈值
    val topN = 8
    //所有的userid
    val userids = model.userFactors.select($"id".as("userid"))
    //所有的appid
    val appids = model.itemFactors.select($"id".as("appid"))
    //获取结算app
    val ourappMapping = buildAppMapping(recommendAppList)
    ourappMapping.createOrReplaceTempView("ourappMapping")
    val ourappids = sql("select appid from ourappMapping")
    ourappids.createOrReplaceTempView("ourappids")
    //提取在appids里的结算appids
    val ourleftappids = sql("select a.appid from ourappids a inner join appMapping b on a.appid = b.appid")
    //得到用户没有接触过的结算app集合
    val left = userids.join(ourleftappids).except(trainData_format.select("userid", "appid"))
    //获取每个用户topN的app推荐
    val w = Window.partitionBy($"userid").orderBy($"prediction".desc)
    val result = model.transform(left)
      .select("userid", "appid", "prediction")
      .withColumn("rn", row_number.over(w)).where($"rn" <= topN).drop("rn")
    result.createOrReplaceTempView("result")
    //转换为user:String,app:String,rating:String格式的数据
    //864087037299098 	com.uc.addon.processkiller	1.2187707
    val result_format = sql("select b.user,c.app,a.prediction from result a left join userMapping b on a.userid = b.userid left join ourappMapping c on a.appid = c.appid")
    result_format.rdd
  }

  /**
    * 得出推荐结果
    *
    * @param rawUserAppData
    */
  def calcPrediction(rawUserAppData: DataFrame, folder: String): RDD[Row] = {

    // 推荐列表
    val recommendAppList = DbUtil.getRecommendApp(spark, folder)
    //初始化样本数据，转化为user:Stirng,app:Stirng,rating:Double
    //    val trainData = rawUserAppData
    //构造String id 到整形id的映射(基于hashcode)
    val userMapping = buildUserMapping(rawUserAppData)
    val appMapping = buildAppMapping(rawUserAppData)
    userMapping.createOrReplaceTempView("userMapping")
    appMapping.createOrReplaceTempView("appMapping")
    rawUserAppData.createOrReplaceTempView("trainData")
    //转为userid:Int,appid:Int,rating:Double格式的数据
    val trainData_format = sql("select b.userid,c.appid,a.rating from trainData a left join userMapping b on a.user = b.user left join appMapping c on a.app = c.app")
    //初始化ALS模块，设置参数
    val model = new ALS().setRank(12).setMaxIter(10).setRegParam(0.2).setUserCol("userid").setItemCol("appid").setRatingCol("rating").setPredictionCol("prediction") //预测列
      .fit(trainData_format)
    //topN阈值
    val topN = 8
    //所有的userid
    val userids = model.userFactors.select($"id".as("userid"))
    //所有的appid
    val appids = model.itemFactors.select($"id".as("appid"))
    //获取结算app
    val ourappMapping = buildAppMapping(recommendAppList)
    ourappMapping.createOrReplaceTempView("ourappMapping")
    val ourappids = sql("select appid from ourappMapping")
    ourappids.createOrReplaceTempView("ourappids")
    //提取在appids里的结算appids
    val ourleftappids = sql("select a.appid from ourappids a inner join appMapping b on a.appid = b.appid")
    //得到用户没有接触过的结算app集合
    val left = userids.join(ourleftappids).except(trainData_format.select("userid", "appid"))
    //获取每个用户topN的app推荐
    val w = Window.partitionBy($"userid").orderBy($"prediction".desc)
    val result = model.transform(left)
      .select("userid", "appid", "prediction")
      .withColumn("rn", row_number.over(w)).where($"rn" <= topN).drop("rn")
    result.createOrReplaceTempView("result")
    //转换为user:String,app:String,rating:String格式的数据
    //864087037299098 	com.uc.addon.processkiller	1.2187707
    val result_format = sql("select b.user,c.app,a.prediction from result a left join userMapping b on a.userid = b.userid left join ourappMapping c on a.appid = c.appid")
    result_format.rdd
  }

  /**
    * 推荐及入库全过程
    *
    * @param rawData
    * @param imeis
    * @param folder
    */
  def recommend(rawData: DataFrame, imeis: RDD[(String, String)], folder: String): RDD[(String, String)] = {
    // 需要推荐的APP列表
    val appList = DbUtil.getRecommendApp(spark, folder)
    val appRdd = appList.rdd.map(a => {
      val pkg = a.getString(0)
      val downloadurl = a.getString(1)
      val appname = a.getString(2)
      val icon = a.getString(3)
      val csize = a.getDouble(4)
      val cversion = a.getInt(5)

      (pkg, (pkg, appname, downloadurl, icon, csize, cversion))
    })

    // 预测的结果数据
    val predicts = calcPrediction(rawData, appList)

    // 推荐结果
    val recommend = predicts.map(f => {
      val imei = f.getString(0)
      val pkg = f.getString(1)
      val prediction = f.getFloat(2)
      (imei, (pkg, prediction))
    }
    ).join(imeis).map(f => {
      val imeiid = f._2._2
      val pkg = f._2._1._1
      val prediction = f._2._1._2
      (pkg, (imeiid, prediction))
    }).join(appRdd)
      .map(f => {
        val appData = f._2._2
        val imeiid = f._2._1._1
        val prediction = f._2._1._2
        (imeiid, (prediction, appData) :: Nil)
      }).reduceByKey(_ ::: _)
      .map(f => {
        val imeiid = f._1
        val appDatas = f._2.sortBy(_._1)
        val arr = new JSONArray()
        // 反向遍历
        for (i <- 0 until appDatas.length reverse) {
          val obj = new JSONObject()

          val pkgname = appDatas(i)._2._1
          val appname = appDatas(i)._2._2
          val durl = appDatas(i)._2._3
          val icon = appDatas(i)._2._4
          val csize = appDatas(i)._2._5
          val cversion = appDatas(i)._2._6
          val prediction = appDatas(i)._1
          obj.put("appname", appname)
          obj.put("pkgname", pkgname)
          obj.put("durl", durl)
          obj.put("icon", icon)
          obj.put("csize", csize)
          obj.put("cversion", cversion)
          obj.put("prediction", prediction)
          arr.add(obj)
        }
        (imeiid, arr.toJSONString())
      })
    recommend
  }

  /**
    * 推荐及入库全过程
    *
    * @param imeis
    * @param folder
    */
  def recommend(hdfsPath: String, imeis: RDD[(String, String)], folder: String): RDD[(String, String)] = {

    // 需要推荐的APP列表
    val appList = DbUtil.getRecommendApp(spark, folder)
    val appRdd = appList.rdd.map(a => {
      val pkg = a.getString(0)
      val downloadurl = a.getString(1)
      val appname = a.getString(2)
      val icon = a.getString(3)
      val csize = a.getDouble(4)

      (pkg, (pkg, appname, downloadurl, icon, csize))
    })

    val predicts = spark.sparkContext.textFile(hdfsPath + folder)
      .map(r => {
        val arr = r.split("\\|")
        (arr(0), (arr(1), arr(2)))
      })

    // 推荐结果
    val recommend = predicts.join(imeis).map(f => {
      val imeiid = f._2._2
      val pkg = f._2._1._1
      val prediction = f._2._1._2
      (pkg, (imeiid, prediction))
    }).join(appRdd)
      .map(f => {
        val appData = f._2._2
        val imeiid = f._2._1._1
        val prediction = f._2._1._2
        (imeiid, (prediction, appData) :: Nil)
      }).reduceByKey(_ ::: _)
      .map(f => {
        val imeiid = f._1
        val appDatas = f._2.sortBy(_._1)
        val arr = new JSONArray()
        // 反向遍历
        for (i <- 0 until appDatas.length reverse) {
          val obj = new JSONObject()

          val pkgname = appDatas(i)._2._1
          val appname = appDatas(i)._2._2
          val durl = appDatas(i)._2._3
          val icon = appDatas(i)._2._4
          val csize = appDatas(i)._2._5
          val prediction = appDatas(i)._1
          obj.put("appname", appname)
          obj.put("pkgname", pkgname)
          obj.put("durl", durl)
          obj.put("icon", icon)
          obj.put("csize", csize)
          obj.put("prediction", prediction)
          arr.add(obj)
        }
        (imeiid, arr.toJSONString())
      })
    recommend
  }

  /**
    * 推荐结果存入数据库
    *
    * @param spark
    * @param result
    * @param allImei
    * @param tablename
    */
  def resultToDB(spark: SparkSession, result: RDD[(String, String)], allImei: RDD[(String, List[String])], tablename: String, folder:String): Unit = {
    import spark.implicits._

    //保存前先清空表
    DbUtil.cleanTable(DbUtil.RECOMMEND_DB_URL, DbUtil.RECOMMEND_DB_USERNAME, DbUtil.RECOMMEND_DB_PASSWORD, tablename)
    val prop = new java.util.Properties
    prop.setProperty("user", DbUtil.RECOMMEND_DB_USERNAME)
    prop.setProperty("password", DbUtil.RECOMMEND_DB_PASSWORD)

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    if (StringUtils.isNotBlank(folder)) {
      val recommend = result.join(allImei).flatMap(f => {
        val list = ArrayBuffer[(String, String, String, String)]()
        val appData = f._2._1
        val imeis = f._2._2
        for (i <- imeis)
          list += ((EncodeUtil.getMD5(i), appData, sdf.format(new Date()), folder))
        list
      }).toDF("imei", "appdata", "addtime", "folder")

      recommend.coalesce(10)
        .write.mode(SaveMode.Append)
        .jdbc(DbUtil.RECOMMEND_DB_URL, tablename, prop)
    } else {
      val recommend = result.join(allImei).flatMap(f => {
        val list = ArrayBuffer[(String, String, String)]()
        val appData = f._2._1
        val imeis = f._2._2
        for (i <- imeis)
          list += ((i, appData, sdf.format(new Date())))
        list
      }).toDF("imei", "appdata", "addtime")

      recommend.coalesce(10)
        .write.mode(SaveMode.Append)
        .jdbc(DbUtil.RECOMMEND_DB_URL, tablename, prop)
    }

  }

  def folderResultToDB(spark: SparkSession, result: RDD[(String, (String, String, String, String, String, String))], allImei: RDD[(String, List[String])], table: String): Unit = {
    import spark.implicits._

    //保存前先清空表
    DbUtil.cleanTable(DbUtil.RECOMMEND_DB_URL, DbUtil.RECOMMEND_DB_USERNAME, DbUtil.RECOMMEND_DB_PASSWORD, table)
    val prop = new java.util.Properties
    prop.setProperty("user", DbUtil.RECOMMEND_DB_USERNAME)
    prop.setProperty("password", DbUtil.RECOMMEND_DB_PASSWORD)

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val recommend = result.join(allImei).flatMap(f => {
      val list = ArrayBuffer[(String, String, String, String, String, String, String, String)]()
      val appData = f._2._1
      val rmd_201 = appData._1
      val rmd_202 = appData._2
      val rmd_203 = appData._3
      val rmd_204 = appData._4
      val rmd_205 = appData._5
      val rmd_206 = appData._6

      val imeis = f._2._2
      for (i <- imeis)
        list += ((EncodeUtil.getMD5(i), sdf.format(new Date()), rmd_201, rmd_202, rmd_203, rmd_204, rmd_205, rmd_206))
      list
    }).toDF("imei", "addtime", "rmd_201", "rmd_202", "rmd_203", "rmd_204", "rmd_205", "rmd_206")

    recommend.coalesce(10)
      .write.mode(SaveMode.Append)
      .jdbc(DbUtil.RECOMMEND_DB_URL, table, prop)

  }
}
