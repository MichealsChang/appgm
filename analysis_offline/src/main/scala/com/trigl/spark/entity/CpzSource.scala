package com.trigl.spark.entity

class CpzSource(
                 val postdate: String,
                 val operationdate: String,
                 val operationtype: String,
                 val imid: String,
                 val channelid: String,
                 var phonemodel: String,
                 val cooperationtype: String,
                 val cost: String,
                 val ctype: String,
                 val ime1: String,
                 val ime2: String,
                 val currencytype: String,
                 var state: String,
                 val spid: String,
                 val creatorid: String,
                 val adddate: String,
                 val ccount: String,
                 val chcount: String,
                 val simstate: String,
                 val linktype: String,
                 val manufacturid: String,
                 val manufacturname: String,
                 val cdate: String, // 客户端做货时间
                 val phonemodelname: String,
                 val verid: String,
                 val packagelist: String,
                 val cpzsourceid: String,
                 val customid: String,
                 val romid: String,
                 val romtype: String,
                 val packagelist_copy: String,
                 val companyid: String,
                 var systemappcount: String,
                 var userappcount: String,
                 val clienttype: String,
                 val supportflash: String,
                 var appList: List[CpzApp]) {

  //app个数
  var appcount: Int = 0
  //是否新数据
  var isNew: Int = 0
  //hbase key
  var key: String = null
  //用户账号
  var acc: String = null

  var brand: String = null //品牌

  var scriptmode: String = null

}