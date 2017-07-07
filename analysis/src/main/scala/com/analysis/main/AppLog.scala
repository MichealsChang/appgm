package com.analysis.main

/**
  *
  * Created by 白鑫 on 2017-04-07 16:38
  */
class AppLog {

  //数据来源标识
  var source: String = null
  //预留，暂时没用到
  var unUsed: String = null
  //处理方式
  var processType: String = null
  //IP地址
  var serverIp: String = null
  //手机操作时间
  var optTime: String = null
  //服务提供商
  var serviceProvider: String = null
  //网络类型
  var netType: String = null
  //IMEI个数
  var imeiCount: String = null
  //IMEI1内容
  var firstImei: String = null
  //IMEI2内容
  var secondImei: String = null
  //IMEI3内容
  var thirdImei: String = null
  //渠道
  var channelNumber: String = null
  //标识
  var flag: String = null
  //手机品牌内容
  var phoneBrand: String = null
  //手机型号内容
  var phoneModel: String = null
  //软件列表内容
  var appList: String = null
  //计数器包名
  var counterPkg: String = null
  //当前软件CRC32内容
  var counterCrc32: String = null
  //经度
  var longitude: String = null
  //纬度
  var latitude: String = null
  //手机获取的IP地址
  var phoneIp: String = null
  //SIM卡状态
  var simStatus: String = null
  //sim卡序列号
  var simSequence: String = null
  //手机的唯一标识
  var identity: String = null
  //手机到达次数
  var arriveTimes: String = null
  //服务器接收时间时间戳
  var acceptTimestamp: String = null

  //additional
  //解析状态（1：成功，0：失败）
  var parseState: Int = 0
  //原始数据，用于异常处理
  var rawData: String = null

  def printStr(): String ={
    processType + "|" + parseState + "|" + serverIp + "|" + optTime + "|" + phoneBrand + "|" + appList + "|" + counterPkg
  }
}
