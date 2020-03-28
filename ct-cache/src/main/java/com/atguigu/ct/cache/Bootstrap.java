package com.atguigu.ct.cache;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.atguigu.ct.common.util.JDBCUtil;

import redis.clients.jedis.Jedis;

/**
 * 启动缓存
 * 
 * @author Administrator
 *
 */
public class Bootstrap {

	public static void main(String[] args) {
		Map<String, Integer> userMap = new HashMap<String, Integer>();
		Map<String, Integer> dateMap = new HashMap<String, Integer>();
		// 获取资源
		Connection connection = null;
		connection = JDBCUtil.getConnection();
		PreparedStatement pstat = null;
		ResultSet rs = null;

		try {

			String queryUserSql = "select id, tel from ct_user";
			pstat = connection.prepareStatement(queryUserSql);
			rs = pstat.executeQuery();
			while (rs.next()) {
				Integer id = rs.getInt(1);
				String tel = rs.getString(2);
				userMap.put(tel, id);
			}

			rs.close();

			String queryDateSql = "select id, year, month, day from ct_date";
			pstat = connection.prepareStatement(queryDateSql);
			rs = pstat.executeQuery();
			while (rs.next()) {
				Integer id = rs.getInt(1);
				String year = rs.getString(2);
				String month = rs.getString(3);
				if (month.length() == 1) {
					month = "0" + month;
				}
				String day = rs.getString(4);
				if (day.length() == 1) {
					day = "0" + day;
				}
				dateMap.put(year + month + day, id);
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (pstat != null) {
				try {
					pstat.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (connection != null) {
				try {
					connection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
//		System.out.println(userMap.size());
//		System.out.println(dateMap.size());
		
		//向redis中存入数据
		Jedis jedis = new Jedis("hadoop102",6379);
		Iterator<String> iterator = userMap.keySet().iterator();
		while(iterator.hasNext()) {
			String key = iterator.next();
			Integer  value= userMap.get(key);
			jedis.hset("ct_user", key, ""+ value);
		}
		
		Iterator<String> dateIterator = dateMap.keySet().iterator();
		while(dateIterator.hasNext()) {
			String key = dateIterator.next();
			Integer  value= dateMap.get(key);
			jedis.hset("ct_date", key, ""+ value);
		}
	}
}
