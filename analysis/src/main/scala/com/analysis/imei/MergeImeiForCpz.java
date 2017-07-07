package com.analysis.imei;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
/**
 * 
 * @author lanny
 * 广告主IMEI库入库
 *
 */
public class MergeImeiForCpz {

	
	private static String NULL_IMEI1="00000000000000";
	private static String NULL_IMEI2="000000000000000";
	
	private static String IMEI_TABLE="t_imei_cpz";
    private static String IMEI_LOG_TABLE ="t_imei_log";
    
    private static final int IMEI_TYPE_ARR=1;
    private static final int IMEI_TYPE_CPZ=2;
    private static final int IMEI_TYPE_ALL=3;

	// args:/data/imeis/imeis_cpz/2017/05/04/09/imeis2017050409
	public static void main(String[] args) throws SQLException,
			ClassNotFoundException {
		//file
		if(args==null || args.length<1){
			System.err.println("参数异常");
			System.exit(1);
		}
		Date startTime = new Date();

		//输入文件名的末10位  2016101202（年月日时）
		String year_month_day_hour = args[0].substring(args[0].length()-10);

		String url = "jdbc:mysql://183.129.178.156:3306/IMEI?useUnicode=true&characterEncoding=UTF8";
		String username = "root";
		String password = "changmi890*()";

		Class.forName("com.mysql.jdbc.Driver");

		Connection con = DriverManager.getConnection(url, username, password);
		Statement stmt = con.createStatement();

		String sql_upt = "update "+IMEI_TABLE+" set imeiid=? ,num=? where imei = ?";
		PreparedStatement ps_upt  = con.prepareStatement(sql_upt);

		String sql_ins = "insert into "+IMEI_TABLE+"(imei,imeiid,num)values(?,?,?)";
		PreparedStatement ps_ins  = con.prepareStatement(sql_ins);
		
		String sql_ins_log = "insert into "+IMEI_LOG_TABLE+"(timeflag,sourcefrom,type,createtime,timeused,dest)values(?,?,?,?,?,?)";
		PreparedStatement ps_ins_log  = con.prepareStatement(sql_ins_log);
		// imei临时map，存每行的所有imei
		// imei,flag flag:0,imei库里没有，1：imei库里有,需要更新，默认为0
		Map<String, String> imeiTempMap = new HashMap<String, String>();
		
		Set<String> imeiid_needupdate = new HashSet<String>();
		
		Set<String> imeiids = new HashSet<String>();
		File file = new File(args[0]);
		BufferedReader reader = null;
		try {
			System.out.println("以行为单位读取文件内容，一次读一整行：");
			reader = new BufferedReader(new FileReader(file));
			String tempString = null;
			int line = 1;
			// 一次读入一行，直到读入null为文件结束
			while ((tempString = reader.readLine()) != null) {
				// 显示行号
				System.out.println(year_month_day_hour+" imei cpz line " + line);
				line++;
				if(StringUtils.isBlank(tempString))
					continue;
				try {
					// 生成imeiid
					String newImeiid = UUID.randomUUID().toString()
							.replaceAll("-", "");
					String[] arr = tempString.split(",");
					if (arr != null && arr.length > 0) {

						StringBuilder sb = new StringBuilder();
						// 最多三个imei
						for (int j = 0; j < arr.length; j++) {
							String imei = arr[j].trim();
							if (StringUtils.isNotBlank(imei)&& !NULL_IMEI1.equals(imei)&& !NULL_IMEI2.equals(imei)) {
								sb.append(",'" + imei + "'");
								imeiTempMap.put(imei, "");
							}
						}
						if (sb.length() > 0) {
							
							// 查询imei库里的记录
							ResultSet rs = stmt
									.executeQuery("select imei,imeiid from "+IMEI_TABLE+" where imei in ("
											+ sb.substring(1) + ")");
							
							while (rs.next()) {
								//int id = rs.getInt(1);
								String imei = rs.getString(1);
								String imeiid = rs.getString(2);
								// int num = rs.getInt(4);
								if (imeiTempMap.containsKey(imei)) {
									imeiTempMap.put(imei, imeiid);
									// 添加到需要更新的imeiid集合
									imeiid_needupdate.add(imeiid);

								}

							}
							if(imeiid_needupdate.size()>0){
								StringBuilder sb_needupdate = new StringBuilder();
								for (String e : imeiid_needupdate) {
									sb_needupdate.append(",'" + e + "'");
								}
								if (sb_needupdate.length() > 0) {
									// 查询相同imeiid的记录
									ResultSet rs2 = stmt
											.executeQuery("select imei,imeiid from "+IMEI_TABLE+" where imeiid in ("
													+ sb_needupdate.substring(1)
													+ ")");
									while (rs2.next()) {
										//int id = rs2.getInt(1);
										String imei = rs2.getString(1);
										String imeiid = rs2.getString(2);
										// int num = rs2.getInt(4);
										// 设置为需要更新
										imeiTempMap.put(imei, imeiid);
									}
								}
							}
							
							// 记录需要更新的imei
							boolean needInsert = false;
							for (Entry e : imeiTempMap.entrySet()) {
								//String imei = (String) e.getKey();
								String imeiid = (String) e.getValue();
								// 有需要新增的imei
								if (StringUtils.isBlank(imeiid)) {
									needInsert = true;
								} else {
									imeiids.add(imeiid);
								}
							}
							

							if (!needInsert && imeiids.size() < 1) {// imeiTempMap为空，不处理
								continue;
							} else if (needInsert && imeiids.size() < 1) {// 只有需要新增的imei
								//int p=0;
								for (Entry e : imeiTempMap.entrySet()) {
//									if(p++==1)
//										throw new NullPointerException();
									String imei = (String) e.getKey();
									// String imeiid = (String) e.getValue();
									ps_ins.setString(1, imei);
									ps_ins.setString(2, newImeiid);
									ps_ins.setInt(3, imeiTempMap.size());
									ps_ins.execute();
									
									//ps_ins.addBatch();
								}
							} else if (!needInsert && imeiids.size() == 1) {
								// 没有需要新增的，且库里都有的,且imeiid唯一，不用操作
								continue;
							} else if (needInsert && imeiids.size() == 1) {// 有需要新增的，且库里匹配到唯一imeiid的
								for (Entry e : imeiTempMap.entrySet()) {
									String imei = (String) e.getKey();
									String imeiid = (String) e.getValue();
									if (StringUtils.isNotBlank(imeiid)) {
										ps_upt.setString(1, newImeiid);
										ps_upt.setInt(2, imeiTempMap.size());
										ps_upt.setString(3, imei);
										//ps_upt.addBatch();
										ps_upt.execute();
									} else {
										ps_ins.setString(1, imei);
										ps_ins.setString(2, newImeiid);
										ps_ins.setInt(3, imeiTempMap.size());
										//ps_ins.addBatch();
										ps_ins.execute();
									}

								}
							} else if (!needInsert && imeiids.size() > 1) {
								// 没有需要新增的imei，库里匹配到多个imeiid
								for (Entry e : imeiTempMap.entrySet()) {
									String imei = (String) e.getKey();
									// String imeiid = (String) e.getValue();
									// 需要更新的imei
									ps_upt.setString(1, newImeiid);
									ps_upt.setInt(2, imeiTempMap.size());
									ps_upt.setString(3, imei);
									//ps_upt.addBatch();
									ps_upt.execute();
								}
							} else if (needInsert && imeiids.size() > 1) {
								// 有需要新增的imei，库里匹配到多个imeiid
								for (Entry e : imeiTempMap.entrySet()) {
									String imei = (String) e.getKey();
									String imeiid = (String) e.getValue();
									// 需要更新的imei
									if (StringUtils.isNotBlank(imeiid)) {
										ps_upt.setString(1, newImeiid);
										ps_upt.setInt(2, imeiTempMap.size());
										ps_upt.setString(3, imei);
										//ps_upt.addBatch();
										ps_upt.execute();
									} else {
										ps_ins.setString(1, imei);
										ps_ins.setString(2, newImeiid);
										ps_ins.setInt(3, imeiTempMap.size());
										//ps_ins.addBatch();
										ps_ins.execute();
									}

								}
							}

						}
						
						// 批量提交
//						con.setAutoCommit(false);
//						ps_upt.executeBatch();
//						ps_ins.executeBatch();
//						con.commit();
//						con.setAutoCommit(true);
					}
				} catch (Exception e) {
					System.out.println("第" + (line-1) + "行操作失败，失败原因："
							+ e.getMessage());
//					if (!con.isClosed()) {
//						// 提交失败，执行回滚操作
//						con.rollback();
//						con.setAutoCommit(true);
//					}
				} finally {
//					if (ps_upt != null)
//						ps_upt.clearBatch();
//					if (ps_ins != null)
//						ps_ins.clearBatch();
					// 清除imei临时map
					imeiTempMap.clear();
					imeiid_needupdate.clear();
					imeiids.clear();
					
					
				}
				
			}
			System.out.println("imei cpz opt finish");
			
			Date endTime = new Date();
			double timeused = (endTime.getTime()-startTime.getTime())/(1000*60);//单位分钟
			//写入日志表
			
			ps_ins_log.setString(1, year_month_day_hour);
			ps_ins_log.setString(2, args[0]);
			ps_ins_log.setInt(3, IMEI_TYPE_CPZ);//1:到达，2：装刷
			ps_ins_log.setTimestamp(4,  new Timestamp(new Date().getTime()));
			ps_ins_log.setDouble(5, timeused);
			ps_ins_log.setString(6, IMEI_TABLE);
			//ps_upt.addBatch();
			ps_ins_log.execute();
			
			//reader.close();
			
			//MergeImeiForAll.main(new String[]{args[0],String.valueOf(IMEI_TYPE_CPZ)});
			//String s = year_month_day_hour;//2016122712
			String year = year_month_day_hour.substring(0,4);
			String month = year_month_day_hour.substring(4,6);
			String day = year_month_day_hour.substring(6, 8);
			String hour = year_month_day_hour.substring(8, 10);
			String cpz_data_hdfs = "/changmi/cpz_data"+"/"+year+"/"+month+"/"+day+"/"+hour;
			
			//跑提取装刷结果
			Runtime rt = Runtime.getRuntime();
			String[] cmd2 = {"/bin/sh", "-c", 
					" nohup /data/install/spark/bin/spark-submit --master spark://fenxi-xlg:7077 --driver-memory 1G --executor-memory 20G --total-executor-cores 10 --name Data-Analyze --supervise --class com.analysis.test.prd.cpz.ExportCpzSourceToMysqlHourAdjustSync_V2 --jars /data/install/hbase/lib/hbase-client-1.2.2.jar,/data/install/hbase/lib/hbase-server-1.2.2.jar,/data/install/hbase/lib/hbase-common-1.2.2.jar,/data/install/hbase/lib/hbase-protocol-1.2.2.jar,/data/install/hbase/lib/guava-12.0.1.jar,/data/install/hbase/lib/htrace-core-3.1.0-incubating.jar,/data/install/hbase/lib/metrics-core-2.2.0.jar,/home/hadoop/jars/mysql-connector-java-5.1.25.jar,/home/hadoop/jars/fastjson-1.2.1.jar,/home/hadoop/jars/jedis-2.7.2.jar,/home/hadoop/jars/commons-pool2-2.3.jar,/home/hadoop/jars/pinyin4j-2.5.0.jar,/home/hadoop/jars/ojdbc6.jar /home/hadoop/analysis-0.0.1-SNAPSHOT.jar "+cpz_data_hdfs+" > /home/hadoop/spark_mysql.out &"};
			
			try {
				Process proc2 = rt.exec(cmd2);

				proc2.waitFor();
				proc2.destroy();
				System.out.println("跑跑提取装刷结果任务提交");
			}catch(Exception e){
				System.out.println("跑新跑提取装刷结果失败："+e.getMessage());
			}
			
			
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e1) {
				}
			}
			
			if (ps_upt != null)
				ps_upt.close();
			if (ps_ins != null)
				ps_ins.close();
			if (ps_ins_log != null)
				ps_ins_log.close();
			if (con != null)
				con.close();
		}
	}

}