package com.trigl.spark.utils

import java.sql.{Connection, DriverManager, ResultSet, Statement}

import com.trigl.spark.entity.Adjust

import scala.collection.mutable.ArrayBuffer

/**
  * 数据调整工具类
  */
object AdjustUtil {
  def getAdjustConf(): Map[Int, Adjust] = {

    var map = Map[String, Adjust]()
    var con: Connection = null
    var stmt: Statement = null
    var rs: ResultSet = null
    var rs2: ResultSet = null
    var rs3: ResultSet = null
    var mainConf: Adjust = null
    val indivList = ArrayBuffer[Adjust]()
    val customList = ArrayBuffer[Int]()
    try {
      con = DriverManager.getConnection(DbUtil.ADMIN_DB_URL, DbUtil.ADMIN_DB_USERNAME, DbUtil.ADMIN_DB_PASSWORD)
      stmt = con.createStatement()
      //获取主参数
      rs = stmt.executeQuery("select cpzthreshold,cpzratiolowerbound,cpzratioupperbound,ssratio,lsratio,jswratio,mainswitch from t_adjust_main")

      if (rs.next()) {
        val cpzthreshold = rs.getInt(1)
        val cpzratiolowerbound = rs.getInt(2)
        val cpzratioupperbound = rs.getInt(3)
        val ssratio = rs.getInt(4)
        val lsratio = rs.getInt(5)
        val jswratio = rs.getInt(6)
        val mainswitch = rs.getInt(7)
        mainConf = new Adjust(cpzthreshold, cpzratiolowerbound, cpzratioupperbound, ssratio, lsratio, jswratio)
        mainConf.mainswitch = mainswitch
        //获取个性化参数
        rs2 = stmt.executeQuery("select customid,cpzthreshold,cpzratiolowerbound,cpzratioupperbound,ssratio,lsratio,jswratio,indivswitch from t_adjust_indiv")
        while (rs2.next()) {
          val customid = rs2.getInt(1)
          val cpzthreshold = rs2.getInt(2)
          val cpzratiolowerbound = rs2.getInt(3)
          val cpzratioupperbound = rs2.getInt(4)
          val ssratio = rs2.getInt(5)
          val lsratio = rs2.getInt(6)
          val jswratio = rs2.getInt(7)
          val indivswitch = rs2.getInt(8)
          val adjust = new Adjust(cpzthreshold, cpzratiolowerbound, cpzratioupperbound, ssratio, lsratio, jswratio)
          adjust.customid = customid
          adjust.indivswitch = indivswitch
          indivList += adjust
        }
        rs3 = stmt.executeQuery("select clientuserid from t_clientuser")
        while (rs3.next()) {
          //获取所有customid
          val customid = rs3.getInt(1)
          customList += customid
        }

      }

      //mainConf
      //indivList
      //customList
      //总的开，某个客户没有配置，是按总的来
      //总的开，某个客户配置开，那就按这个客户的配置
      //总的开，某个客户配置关了，那这个客户就不做调整
      //总的关，某个客户没有配置，那这个客户就不做调整；
      //总的关，某个客户的开，那就按客户的配置来；
      //总的关，某个客户的关，那这个客户就不做调整
      var map = Map[Int, Adjust]()
      if (mainConf != null) {

        if (mainConf.mainswitch == 1) {
          //主参数开启
          //先设置所有custom的参数为主参数
          for (c <- customList) {
            map += (c -> mainConf)
          }

          //遍历个性化参数，开启的替换，关闭的删除，
          for (indiv <- indivList) {
            if (indiv.indivswitch == 1) {
              //开启开关
              map += (indiv.customid -> indiv)
            } else if (indiv.indivswitch == 0) {
              //关闭开关
              map -= (indiv.customid)
            }
          }
        } else if (mainConf.mainswitch == 0) {
          //主参数关闭
          //遍历个性化参数，开启添加，关闭不做调整，
          for (indiv <- indivList) {
            if (indiv.indivswitch == 1) {
              //开启开关
              map += (indiv.customid -> indiv)
            }
          }
        }

      }

      //修正数据
      for (m <- map) {
        val adjust = m._2
        if (adjust.cpzthreshold < 0)
          adjust.cpzthreshold = 0
        else if (adjust.cpzthreshold > 100)
          adjust.cpzthreshold = 100
        if (adjust.cpzratiolowerbound < 0)
          adjust.cpzratiolowerbound = 0
        else if (adjust.cpzratiolowerbound > 100)
          adjust.cpzratiolowerbound = 100
        if (adjust.cpzratioupperbound < 0)
          adjust.cpzratioupperbound = 0
        else if (adjust.cpzratioupperbound > 100)
          adjust.cpzratioupperbound = 100
        if (adjust.cpzratiolowerbound > adjust.cpzratioupperbound)
          adjust.cpzratiolowerbound = adjust.cpzratioupperbound
        if (adjust.ssratio < 0)
          adjust.ssratio = 0
        else if (adjust.ssratio > 100)
          adjust.ssratio = 100
        if (adjust.lsratio < 0)
          adjust.lsratio = 0
        else if (adjust.lsratio > 100)
          adjust.lsratio = 100
        if (adjust.jswratio < 0)
          adjust.jswratio = 0
        else if (adjust.jswratio > 100)
          adjust.jswratio = 100
      }
      map
    } catch {
      case e: Exception =>
        System.out.println("获取需要调整的客户列表失败:" + e.getMessage)
        null
    } finally {
      if (rs != null)
        rs.close()
      if (rs2 != null)
        rs2.close()
      if (rs3 != null)
        rs3.close()
      if (stmt != null)
        stmt.close()
      if (con != null)
        con.close()
    }
  }
}