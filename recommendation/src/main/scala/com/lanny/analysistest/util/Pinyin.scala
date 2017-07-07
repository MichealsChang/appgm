package com.lanny.analysistest.util

import net.sourceforge.pinyin4j.format.HanyuPinyinToneType
import net.sourceforge.pinyin4j.format.HanyuPinyinCaseType
import net.sourceforge.pinyin4j.format.HanyuPinyinOutputFormat
import net.sourceforge.pinyin4j.PinyinHelper
import scala.collection.mutable.Map
object Pinyin {
  
  val map = Map[String,String]()
  map += ("北京"->"beijing")
  map += ("天津"->"tianjin")
  map += ("上海"->"shanghai")
  map += ("重庆"->"chongqing")
  map += ("河北"->"hebei")
  map += ("河南"->"henan")
  map += ("云南"->"yunnan")
  map += ("辽宁"->"liaoning")
  map += ("黑龙江"->"heilongjiang")
  map += ("湖南"->"hunan")
  map += ("安徽"->"anhui")
  map += ("山东"->"shandong")
  map += ("新疆"->"xinjiang")
  map += ("江苏"->"jiangsu")
  map += ("浙江"->"zhejiang")
  map += ("江西"->"jiangxi")
  map += ("湖北"->"hubei")
  map += ("广西"->"guangxi")
  map += ("甘肃"->"gansu")
  map += ("山西"->"shan1xi")
  map += ("内蒙古"->"neimenggu")
  map += ("陕西"->"shan3xi")
  map += ("吉林"->"jilin")
  map += ("福建"->"fujian")
  map += ("贵州"->"guizhou")
  map += ("广东"->"guangdong")
  map += ("青海"->"qinghai")
  map += ("西藏"->"xizang")
  map += ("四川"->"sichuan")
  map += ("宁夏"->"ningxia")
  map += ("海南"->"hainan")
  map += ("台湾"->"taiwan")
  map += ("香港"->"xianggang")
  map += ("澳门"->"aomen")
  
    /**
    * 汉字转拼音的方法
    * @param name 汉字
    * @return 拼音
    */
  //def toPut(result:(String,Result))={
    def HanyuToPinyin(name:String)={
    
        //var pinyinName = ""
        var pinyinName = new StringBuilder()
        val nameChar = name.toCharArray();
        val defaultFormat = new HanyuPinyinOutputFormat();
        defaultFormat.setCaseType(HanyuPinyinCaseType.LOWERCASE);
        defaultFormat.setToneType(HanyuPinyinToneType.WITHOUT_TONE);
        for (i<- 0 to nameChar.length-1) {
            if (nameChar(i) > 128) {
                pinyinName.append(PinyinHelper.toHanyuPinyinStringArray(nameChar(i), defaultFormat)(0))
            } 
        }
        pinyinName.toString;
    }
 
    def main(args: Array[String]) {
        System.out.println(HanyuToPinyin("重庆"));
    }    
}