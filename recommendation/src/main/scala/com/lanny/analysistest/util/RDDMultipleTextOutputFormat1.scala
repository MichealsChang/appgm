package com.lanny.analysistest.util

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.io.NullWritable

class RDDMultipleTextOutputFormat1 extends MultipleTextOutputFormat[Any, Any] {
  override def generateActualKey(key: Any, value: Any): Any = 
    NullWritable.get()
  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String =  
    key.asInstanceOf[String]

}