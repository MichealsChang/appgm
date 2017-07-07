package com.analysis.install

class CpzApp (
val appid : String
,val appdetailid : String
,val imid : String
,val state : String
,val adddate : String
,val cpzasourceid : String
,val areatype : String
,val cdate : String
,val ime1 : String
,val ime2 : String
){
  var pkg:String=null
  
  var impandsuc:String=null//重要的app，且安装成功的，分离模式下使用，主类scriptmode=1，非分离模式下值都是0
}