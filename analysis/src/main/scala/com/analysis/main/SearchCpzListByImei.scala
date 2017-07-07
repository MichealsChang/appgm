package com.analysis.main

import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.text.SimpleDateFormat

import com.analysis.util.DbUtil
import org.apache.hadoop.hbase.client.{ConnectionFactory, Result, Scan}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

import scala.collection.mutable.ArrayBuffer

object SearchCpzListByImei {
  val TABLE_NAME = "cpz_source_v2";
  val COLUMN_FAMILY = "scf";
  val HBASE_ZOOKEEPER_QUORUM = "fenxi-xlg";
  val HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT = "2181";
  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }
  def main(args: Array[String]) {

     val in = List[String](
"A00000760DEA9A",
"a000006d71eb4b"
    )
    
    val lst = getCompImei(in)
    
        val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", HBASE_ZOOKEEPER_QUORUM)
    conf.set("hbase.zookeeper.property.clientPort", HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT)
    conf.set(TableInputFormat.INPUT_TABLE, TABLE_NAME)
    conf.setInt("hbase.rpc.timeout", 120000)
    conf.setInt("hbase.client.operation.timeout", 120000)
    conf.setInt("hbase.client.scanner.timeout.period", 120000)
    //conf.setInt("hbase.regionserver.lease.period", 12000)
    //println(">>>>>>>>>>>>>>>>>>>>search Imei:" + args(0))
    val scan = new Scan()
    scan.setCaching(10000)
		scan.setCacheBlocks(false);
    //scan.addColumn(COLUMN_FAMILY.getBytes, "spid".getBytes)
    scan.addColumn(COLUMN_FAMILY.getBytes, "packagelist".getBytes)
    //scan.addColumn(COLUMN_FAMILY.getBytes, "ctype".getBytes)
    scan.addColumn(COLUMN_FAMILY.getBytes, "cdate".getBytes)
    //scan.addColumn(COLUMN_FAMILY.getBytes, "customid".getBytes)
    val sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    
        val conn = ConnectionFactory.createConnection(conf)
    val table = conn.getTable(TableName.valueOf(TABLE_NAME));
    var count = 0
    var res = new StringBuilder()
    import java.io._
    for(a<-lst){
      count = count+1
      println("count:"+count)
      val imeis = a.split(",")
      val arrList = ArrayBuffer[(String,(String,String,String,String,String))]()
      for(i<-imeis){
        scan.setRowPrefixFilter(i.getBytes)
        val resultScanner = table.getScanner(scan);
        val it = resultScanner.iterator()
        val sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        if (it.hasNext) {
          val result: Result = it.next()
    
          val key = Bytes.toString(result.getRow)
          //val spid = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "spid".getBytes))
          val cdate = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "cdate".getBytes))
          
          //val ctype = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "ctype".getBytes))
          val packagelist = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "packagelist".getBytes))
          //val customid = Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes, "customid".getBytes))
 
         
          arrList+=((cdate,(key,"",packagelist,"","")))
        }
        
      }
      val sortList = arrList.sorted
       
      
      if(sortList.length>0){
        val s = sortList(0)
         res.append(""+s._2._1 +"|"+s._2._3+ "|" + s._1 +"\n")
      }
//      for(s<-sortList){
//        res.append("" + s._2._1 +"|"+s._2._3+"|" + s._1 +"\n")
//      }
      //res.append("\n")
      
      if(count%300 ==0){
        val file = new File("D:\\cpzCheck\\zhenzhouzhongkai_check-"+count+".log")
        if(file.exists())
          file.delete()
        val writer = new PrintWriter(file, "UTF-8")
        writer.write(res.toString())
        writer.close()
        
        res.clear()
      }
    }
   
    if(res.length>0){
        val file = new File("D:\\cpzCheck\\zhenzhouzhongkaisi_check-"+count+".log")
        if(file.exists())
          file.delete()
        val writer = new PrintWriter(file, "UTF-8")
        writer.write(res.toString())
        writer.close()
    }

    
    

   // sc.stop()
  }
     //获取初始化基础信息
  def getCompImei(in:List[String]):ArrayBuffer[String]={
      var dbconn: Connection = null  
      var stmt:Statement = null
      var rs:ResultSet = null
      val lst = ArrayBuffer[String]()
      var imeiids = ArrayBuffer[String]()
      try{
        dbconn = DriverManager.getConnection(DbUtil.IMEI_DB_URL, DbUtil.IMEI_DB_USERNAME, DbUtil.IMEI_DB_PASSWORD)
      
        stmt = dbconn.createStatement();

        rs = stmt.executeQuery("select imeiid from t_imei_all where imei in ('"+in.mkString("','")+"')");
        while (rs.next()) {
          val imeiid = rs.getString(1)
          imeiids +=(imeiid)
        }
        if(imeiids.length>0){
          val sql = "select GROUP_CONCAT(imei),imeiid from t_imei_all where imeiid in ('"+imeiids.mkString("','")+"') group by imeiid"
          println(sql)
          rs = stmt.executeQuery(sql)
          while (rs.next()) {
            val imeis = rs.getString(1)
            lst +=(imeis)
          }
        }
      }catch{
        case e:Exception=>
          System.out.println("获取机型失败:"+e.printStackTrace());
      }finally{
        if(rs!=null)
          rs.close()
        if(stmt!=null)
          stmt.close()
        if(dbconn!=null)
          dbconn.close()
      }
      lst
  }
}