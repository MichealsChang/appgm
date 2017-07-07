package com.trigl.spark.utils

import java.io.IOException
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.SparkContext



object HdfsUtil {

  //val MASTER_HOSTNAME = "fenxi-xlg";
  //val HDFS_ROOT = "hdfs://fenxi-xlg:9000";
  val HDFS_ROOT = "hdfs://ns"
  
  //预装解析原始数据保存
  val CPZ_DATA_DIR = "/changmi/cpz_data"
  //预装软件到达
  val CPZ_APP_DATA_DIR = "/changmi/cpz_app_data"
  //老到达解析原始数据保存
  val ARR_DATA_DIR = "/changmi/arrive_data"
  //新到达解析原始数据保存
  val ARR_DATA_DIR_V2 ="/changmi/arrive_data_v2"
  //老到达首次二次数据
  //val FIRST_AND_SECOND_DIR="/changmi/FirstAndSecond"
  //新到达首次二次数据
  //val FIRST_AND_SECOND_DIR_V2="/changmi/FirstAndSecond_V2"
  //软件到达新增数据（天湃）
  val ADD_APP_DIR = "/changmi/add_app"
  //软件到达新增数据匹APP数据（天湃，数据来源/changmi/add_app）
  val ADD_APP_DETAIL_TIANPAI_DIR = "/changmi/add_app_detail/tianpai"
  //应用市场新老系统结算app数据
  val APPCENTER_DIR = "/changmi/appcenter"
  //软件到达新增数据匹APP数据（应用市场，数据来源/changmi/appcenter）
  val ADD_APP_DETAIL_CHANGMI_DIR = "/changmi/add_app_detail/changmi"
  
  //广告主维度软件到达明细
  val ADD_APP_DETAIL_MATCH_CPZ_DIR = "/changmi/add_app_detail/match_cpz"
  //广告主维度软件到达统计
  val ADD_APP_DETAIL_MATCH_CPZ_STAT_DIR = "/changmi/add_app_detail/match_cpz_stat"
  //运营维度软件到达明细
  val ADD_APP_DETAIL_MATCH_CPZ_FOR_ADMIN_DIR = "/changmi/add_app_detail/match_cpz_for_admin"
  //运营维度软件到达统计
  val ADD_APP_DETAIL_MATCH_CPZ_STAT_FOR_ADMIN_DIR = "/changmi/add_app_detail/match_cpz_stat_for_admin"
  
  
  
    //老到达首次二次数据_Beta
  val FIRST_AND_SECOND_BETA_DIR = "/changmi/FirstAndSecond_Beta"
  //新到达首次二次数据
  val FIRST_AND_SECOND_BETA_DIR_V2 = "/changmi/FirstAndSecond_Beta_V2"
  
  //渠道维度老到达未匹配到预装的数据
  val CHANNEL_ARR_NO_MATCH_CPZ_DIR = "/changmi/channel_arr_no_match_cpz"
  //渠道维度新到达未匹配到预装的数据
  val CHANNEL_ARR_NO_MATCH_CPZ_DIR_V2 = "/changmi/channel_arr_no_match_cpz_v2"
  
  val DUPLICATE_CPZ_DIR="/changmi/duplication_cpz"
  
  
  val IMEIS_ARR_DIR = "/changmi/imeis_arr"
  val IMEIS_CPZ_DIR = "/changmi/imeis_cpz"
  //imei_all的hdfs缓存，来源于imei库，每天获取一次
  val IMEI_ALL_BY_DAY_DIR = "/changmi/imei_all_by_day"
  
def deleteDir(sc: SparkContext, path: String) = {
    val hdfs = FileSystem.get(new URI(HDFS_ROOT), sc.hadoopConfiguration)
    try {
      hdfs.delete(new Path(path), true) //这里已经new 目录了，删除再说，总之是删了  
      println("输出目录存在，删除掉:%s".format(path))
      //logInfo("输出目录存在，删除掉:%s".format(outpath))  
    } catch {
      case _: Throwable =>
        println("输出目录不存在，不用删除")
    }
  }
  
    
def checkDirExist(sc: SparkContext, path: String):Boolean = {
    val hdfs = FileSystem.get(new URI(HDFS_ROOT), sc.hadoopConfiguration)
    val flag = hdfs.exists(new Path(path))
    if(flag)
      true
    else
      false
  }

  def persistImeis(sc: SparkContext, outpath: String) = {
    val hdfs = FileSystem.get(new URI(HDFS_ROOT), sc.hadoopConfiguration)
    try {
      hdfs.delete(new Path(outpath), true) //这里已经new 目录了，删除再说，总之是删了  
      println("输出目录存在，删除掉:%s".format(outpath))
      //logInfo("输出目录存在，删除掉:%s".format(outpath))  
    } catch {
      case _: Throwable =>
        println("输出目录不存在，不用删除")
    }
  }
  

def copyMerge(folder:String, file:String) =  {  
  
    val src = new Path(folder);  
    val dst = new Path(file);  
    val conf = new Configuration();  
    try {  
        FileUtil.copyMerge(src.getFileSystem(conf), src,  
                dst.getFileSystem(conf), dst, false, conf, null);  
    } catch {  
      case e:IOException=>
        // TODO Auto-generated catch block  
        e.printStackTrace();  
    }  
}  
}