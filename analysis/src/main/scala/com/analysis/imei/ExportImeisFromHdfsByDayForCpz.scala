package com.analysis.imei

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark._

/**
 * *
 * 从每日装刷数据里提取imei数据，给入imei库程序提供输入
 */
object ExportImeisFromHdfsByDayForCpz {
  //不同环境修改

  val HBASE_ZOOKEEPER_QUORUM = "fenxi-xlg"

  val HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT = "2181"
  val COLUMN_FAMILY = "scf"
  val databytes = Bytes.toBytes(COLUMN_FAMILY)

  val NULL_IMEI = "0000000000000000"

  //val HDFS_PATH = "hdfs://fenxi-xlg:9000"
  val HDFS_IMEIS_DIR = "/changmi/imeis_cpz"
  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

  def main(args: Array[String]) {
    // 参数：/changmi/cpz_data/2016/12/14/11
    if (args == null || args.length < 1) {
      System.err.println("参数异常")
      System.exit(1)
    }

    var tmp = args(0)
    var pos = tmp.lastIndexOf("/")
    val hour = tmp.substring(pos+1)
    
    tmp = tmp.substring(0, pos)
    pos = tmp.lastIndexOf("/")
    val day = tmp.substring(pos+1)
    
    tmp = tmp.substring(0, pos)
    pos = tmp.lastIndexOf("/")
    val month = tmp.substring(pos+1)
    
    tmp = tmp.substring(0, pos)
    pos = tmp.lastIndexOf("/")
    val year = tmp.substring(pos+1)
    val hdfs_output = HDFS_IMEIS_DIR+"/"+year+"/"+month+"/"+day+"/"+hour+"/"+"imeis"+year+month+day+hour

    //System.setProperty("spark.scheduler.mode", "FAIR") 
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    System.setProperty("spark.network.timeout", "300s")
    System.setProperty("spark.storage.memoryFraction", "0.1")
    //System.setProperty("spark.kryoserializer.buffer.max","2000m")
    //System.setProperty("spark.default.parallelism","192")
    val sparkConf = new SparkConf().setAppName("ExportImeisFromHdfsByDayForCpz-"+args(0))
    val sc = new SparkContext(sparkConf)

    val cpzData = sc.textFile(args(0))
    //rowkey&ime1,ime2
    cpzData.map(f=>{
      try{
        val arr = f.split("&")
        if(arr!=null && arr.length>1)
          arr(1)
        else
          null
      }catch{
        case e:Exception=>
          null
      }
    }).filter(f=>{
      if(f!=null)
        true
      else
        false
    })
    .saveAsTextFile(hdfs_output)

    sc.stop()
    println(" into imei start time:"+sdf.format(new Date()))
    //TimeUnit.MINUTES.sleep(3)
    //println(" into imei start time2:"+sdf.format(new Date()))
    val local = "/data/imeis/imeis_cpz/"+year+"/"+month+"/"+day+"/"+hour+"/"
    val filename = hdfs_output.substring(hdfs_output.lastIndexOf("/")+1)
    val dir=new File(local)
		if(!dir.isDirectory()){
			dir.mkdirs()
		}
    println("spark任务结束，执行装刷imei入库")
    
    println("hdfs_output:"+hdfs_output)
    println("local+filename:"+local+filename)
    //调用本地shell命令，将imei入库
		val rt = Runtime.getRuntime()
    //命令需要全路径
		val cmd = Array("/bin/sh", "-c"," /data/install/hadoop/bin/hadoop fs -cat "+hdfs_output+"/* > "+local+filename+";"
		    
		    //执行入装刷imei库操作
        +" /data/install/jdk/bin/java -cp /home/hadoop/imei-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.imei.MergeImeiForCpz "+local+filename+" > /home/hadoop/imeiCpzLog.out"
        )

		try {
			val proc = rt.exec(cmd)

			proc.waitFor()
			proc.destroy()
			println("装刷imei入库提交结束")
		}catch{
		  case e:Exception=>
		    println(args(0)+"装刷imei入库提交失败："+e.getMessage)
		}
  }

}