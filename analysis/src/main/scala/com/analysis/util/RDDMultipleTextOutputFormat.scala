package com.analysis.util

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

class RDDMultipleTextOutputFormat[K, V] extends MultipleTextOutputFormat[K, V] {


  override def generateActualKey(key: K, value: V): K =
    NullWritable.get().asInstanceOf[K]

  override def generateFileNameForKeyValue(key: K, value: V, name: String): String = {
    key.asInstanceOf[String]
  }

}
