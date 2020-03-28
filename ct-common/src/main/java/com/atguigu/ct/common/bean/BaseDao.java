package com.atguigu.ct.common.bean;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.atguigu.ct.common.api.Column;
import com.atguigu.ct.common.api.Rowkey;
import com.atguigu.ct.common.api.TableRef;
import com.atguigu.ct.common.constant.Names;
import com.atguigu.ct.common.constant.ValueConstant;
import com.atguigu.ct.common.util.DateUtil;

/**
 * 基础的数据访问对象
 */
public abstract class BaseDao {

	private ThreadLocal<Connection> connHolder = new ThreadLocal<Connection>();
	private ThreadLocal<Admin> adminHolder = new ThreadLocal<Admin>();

	protected void start() throws Exception {
		getConnection();
		getAdmin();
	}

	protected void end() throws Exception {
		Admin admin = getAdmin();
		if (admin != null) {
			admin.close();
			adminHolder.remove();
		}
		Connection conn = getConnection();
		if (conn != null) {
			conn.close();
			connHolder.remove();
		}
	}

	/**
	 * 创建表，如果有，删除，没有则创建。
	 * 
	 * @param name
	 * @param families
	 */
	protected void createTableXX(String name, String... families) throws Exception {

		createTableXX(name, null,null, families);
	}

	protected void createTableXX(String name,String coprocessorClass,Integer regionCount, String... families) throws Exception {

		Admin admin = getAdmin();
		TableName tableName = TableName.valueOf(name);
		if (admin.tableExists(tableName)) {
			// 表存在，删除表
			deleteTable(name);
		}
		// 创建表
		createTable(name,coprocessorClass, regionCount, families);
	}

	private void createTable(String name,String coprocessorClass, Integer regionCount, String... families) throws Exception {
		Admin admin = getAdmin();
		TableName tableName = TableName.valueOf(name);
		HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
		if (families == null || families.length == 0) {
			families = new String[1];
			families[0] = Names.CF_INFO.getValue();
		}
		for (String family : families) {
			HColumnDescriptor columnDescriptor = new HColumnDescriptor(family);
			tableDescriptor.addFamily(columnDescriptor);
		}
		
		if(coprocessorClass !=null  && !"".equals(coprocessorClass)) {
			tableDescriptor.addCoprocessor(coprocessorClass);
		}
		
		// 增加预分区
		if (regionCount == null || regionCount <= 1) {
			admin.createTable(tableDescriptor);
		} else {
			byte[][] spiltKeys = genspiltKeys(regionCount);
			admin.createTable(tableDescriptor, spiltKeys);
		}
	}
	
	protected List<String[]> getStartStortRowKeys(String tel,String start,String end){
		List<String[]> rowkeyss = new ArrayList<String[]>();
		 String startTime = start.substring(0, 6);
	        String endTime = end.substring(0, 6);

	        Calendar startCal = Calendar.getInstance();
	        startCal.setTime(DateUtil.parse(startTime, "yyyyMM"));

	        Calendar endCal = Calendar.getInstance();
	        endCal.setTime(DateUtil.parse(endTime, "yyyyMM"));

	        while (startCal.getTimeInMillis() <= endCal.getTimeInMillis()) {

	            // 当前时间
	            String nowTime = DateUtil.format(startCal.getTime(), "yyyyMM");

	            int regionNum = genRegionNum(tel, nowTime);

	            String startRow = regionNum + "_" + tel + "_" + nowTime;
	            String stopRow = startRow + "|";

	            String[] rowkeys = {startRow, stopRow};
	            rowkeyss.add(rowkeys);

	            // 月份+1
	            startCal.add(Calendar.MONTH, 1);
	        }
		return rowkeyss;
		
	}

	/**
	 * 生成分区号
	 * 
	 * @param tel
	 * @param date
	 */
	protected int genRegionNum(String tel, String date) {

		String useCode = tel.substring(tel.length() - 4);
		String yearMonth = date.substring(0, 6);
		int userCodeHash = useCode.hashCode();
		int yearMonthHash = yearMonth.hashCode();
		int crc = Math.abs(userCodeHash ^ yearMonthHash);
		int regionNum = crc % ValueConstant.REGION_COUNT;
		return regionNum;
	}
	/*
	 * public static void main(String[] args) { int i =
	 * genRegionNum("18023498976","20180430070339"); System.out.println(i); }
	 */

	/**
	 * 生成分区键
	 * 
	 * @return
	 */
	private byte[][] genspiltKeys(int regionCount) {
		int spilKeysCount = regionCount - 1;
		byte[][] bs = new byte[spilKeysCount][];
		List<byte[]> bsList = new ArrayList<byte[]>();
		for (int i = 0; i < spilKeysCount; i++) {
			String spiltKey = i + "|";
			// System.out.println(spiltKey);
			bsList.add(Bytes.toBytes(spiltKey));
		}
		bsList.toArray(bs);
		return bs;
	}

	/**
	 * 增加对象
	 * 
	 * @param log
	 */
	protected void putData(Object obj) throws Exception {

		// 反射
		Class clazz = obj.getClass();
		TableRef tableRef = (TableRef) clazz.getAnnotation(TableRef.class);
		String tableName = tableRef.value();

		Field[] fs = clazz.getDeclaredFields();
		String stringRowkey = "";
		for (Field f : fs) {
			Rowkey rowkey = f.getAnnotation(Rowkey.class);
			if (rowkey != null) {
				f.setAccessible(true);
				stringRowkey = (String) f.get(obj);
				break;
			}
		}

		Connection conn = getConnection();
		Table table = conn.getTable(TableName.valueOf(tableName));
		Put put = new Put(Bytes.toBytes(stringRowkey));

		for (Field f : fs) {
			Column column = f.getAnnotation(Column.class);
			if (column != null) {
				String family = column.family();
				String colName = column.column();
				if (colName == null || "".equals(colName)) {
					colName = f.getName();
				}
				f.setAccessible(true);
				String value = (String) f.get(obj);

				put.addColumn(Bytes.toBytes(family), Bytes.toBytes(colName), Bytes.toBytes(value));
			}
		}

		// 增加数据
		table.put(put);

		// 关闭表
		table.close();

	}

	/**
	 * 增加数据
	 * @param name
	 * @param put
	 * @throws Exception
	 */
	protected void putData(String name, Put put) throws Exception {
		// 获取表对象
		Connection conn = getConnection();
		Table table = conn.getTable(TableName.valueOf(name));
		// 增加数据
		table.put(put);
		// 关闭资源
		table.close();

	}
	/**
	 * 增加多条数据
	 * @param name
	 * @param put
	 * @throws Exception
	 */
	protected void putData(String name, List<Put> puts) throws Exception {
		// 获取表对象
		Connection conn = getConnection();
		Table table = conn.getTable(TableName.valueOf(name));
		// 增加数据
		table.put(puts);
		// 关闭资源
		table.close();
		
	}

	/**
	 * 删除表格
	 * 
	 * @param name
	 * @throws Exception
	 */
	protected void deleteTable(String name) throws Exception {
		Admin admin = getAdmin();
		TableName tableName = TableName.valueOf(name);
		admin.disableTable(tableName);
		admin.deleteTable(tableName);
	}

	/**
	 * 创建命名空间，存在则不创建，不存在则创建。
	 *
	 * @param name
	 */
	protected void createNamespceNX(String name) throws Exception {
		Admin admin = getAdmin();
		try {
			admin.getNamespaceDescriptor(name);
		} catch (NamespaceNotFoundException e) {
			// e.printStackTrace();
			NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(name).build();
			admin.createNamespace(namespaceDescriptor);
		}

	}

	/**
	 * 获取连接对象
	 */
	protected Connection getConnection() throws Exception {
		Connection conn = connHolder.get();
		if (conn == null) {
			Configuration conf = HBaseConfiguration.create();
			conn = ConnectionFactory.createConnection(conf);
			connHolder.set(conn);
		}
		return conn;
	}

	/**
	 * 获取管理对象
	 */
	protected synchronized Admin getAdmin() throws Exception {
		Admin admin = adminHolder.get();
		if (admin == null) {
			admin = getConnection().getAdmin();
			adminHolder.set(admin);
		}
		return admin;
	}

	/*
	 * public static void main(String[] args) { System.out.println(
	 * genspiltKeys(6)); }
	 */
}