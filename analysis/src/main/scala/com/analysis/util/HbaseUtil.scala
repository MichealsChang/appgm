package com.analysis.util

import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}


object HbaseUtil {
  val HBASE_ZOOKEEPER_QUORUM = "fenxi-xlg"//"fenxi-xlg,fenxi-mrg,fenxi-xxf,fenxi-ptd,fenxi-xz";
  val HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT = "2181"
  
  val COLUMN_FAMILY = "scf"
  val databytes = Bytes.toBytes(COLUMN_FAMILY)
  
  val TABLE_NAME_CPZ_SOURCE = "cpz_source"
  val TABLE_NAME_CPZ_APP = "cpz_app"
  val TABLE_NAME_CPZ_SOURCE_V2 = "cpz_source_v2"
  val TABLE_NAME_CPZ_APP_V2 = "cpz_app_v2"
  val TABLE_NAME_ARR_OLD = "sta"
  
  
   def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }
}