package com.bkjf.flink_example.sink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.bkjf.flink_example.bean.KafkaBinLogEvent;


public class MySQLSink extends RichSinkFunction<KafkaBinLogEvent>{
	private Logger logger = LoggerFactory.getLogger(MySQLSink.class);
	private static final long serialVersionUID = 1L;
	private static final String MYSQL_USER_NAME = "mysqlUserName";
	private static final String MYSQL_PASSWORD = "mysqlPassword";
	private static final String MYSQL_DBURL = "mysqlDBUrl";
	private static final String MYSQL_TABLE_NAME = "mysqlTableName";
	private static final String DRIVER_NAME = "driverName";
	private static final String TIDB_USER_NAME = "tidbUserName";
	private static final String TIDB_PASSWORD = "tidbPassword";
	private static final String TIDB_DBURL = "tidbDBUrl";
	private static final String TIDB_TABLE_NAME = "tidbTableName";
	private static final String TIDB_IMG_TABLE_NAME = "tidbImgTableName";
	
	private Connection tidbConnection = null;
	private PreparedStatement tidbPreparedStatement = null;
	private String tidbTableName = null;
	private String tidbImgTableName = null;
	
	private Connection mysqlConnection = null;
	private PreparedStatement mysqlPreparedStatement = null;
	private String mysqlTableName = null;
	
	
	private Map<String, List<String>> sysParameterMap;
	private ParameterTool dbParameterTool;
	
	public MySQLSink(Map<String, List<String>> sysParameterMap,ParameterTool mysqlParameterTool) {
		this.sysParameterMap = sysParameterMap;
		this.dbParameterTool = mysqlParameterTool;
	}
	
	@Override
	public void open(Configuration parameters) throws Exception {
		Class.forName(dbParameterTool.get(DRIVER_NAME));
		tidbConnection = DriverManager.getConnection(dbParameterTool.get(TIDB_DBURL), dbParameterTool.get(TIDB_USER_NAME), dbParameterTool.get(TIDB_PASSWORD));
		tidbTableName = dbParameterTool.get(TIDB_TABLE_NAME);
		tidbImgTableName = dbParameterTool.get(TIDB_IMG_TABLE_NAME);
		
		mysqlConnection = DriverManager.getConnection(dbParameterTool.get(MYSQL_DBURL), dbParameterTool.get(MYSQL_USER_NAME), dbParameterTool.get(MYSQL_PASSWORD));
		mysqlTableName = dbParameterTool.get(MYSQL_TABLE_NAME);
		System.out.println("------------------连接db");
	}
	
	@Override
	public void invoke(KafkaBinLogEvent bean) throws Exception {
		String tableName = bean.getTableName();
		List<String> columnList = sysParameterMap.get(tableName);
		if(columnList == null || columnList.size() == 0) {
			return;
		}
		String sql = "";
		if("INSERT".equals(bean.getOperType()) || "UPDATE".equals(bean.getOperType())) {
			if("order_house_info".equals(tableName)) {
				processOrderHouseInfo(columnList, bean.getData().getAfter());
			}
			if("order_spare_part_img".equals(tableName)) {
				processOrderSparePartImg(columnList, bean.getData().getAfter());
			}
			String sqlType = "INSERT INTO "+tidbTableName;
			sql = getSqlStr(columnList,sqlType,bean.getData().getAfter());
			if(StringUtils.isEmpty(sql)) {
				return;
			}
			logger.info("往mysql中插入数据，sql = "+sql);
			tidbPreparedStatement = tidbConnection.prepareStatement(sql);
			tidbPreparedStatement.executeUpdate();
		}else if("DELETE".equals(bean.getOperType())) {
			//TODO
		}
	}
	
	private void processOrderHouseInfo(List<String> columnList,Map<String, String> data) throws SQLException {
		String order_no = data.get("order_no");
		if(StringUtils.isEmpty(order_no)) {
			return;
		}
		StringBuilder sb = new StringBuilder("select pledge_info from ");
		sb.append(tidbTableName).append(" where order_no = ").append(order_no);
		tidbPreparedStatement = tidbConnection.prepareStatement(sb.toString());
		ResultSet rst = tidbPreparedStatement.executeQuery();
		String pledge_info = "";
		if(rst.next()) {
			pledge_info = rst.getString("pledge_info");
		}
		JSONArray newArray = new JSONArray();
		
		JSONObject newObj = new JSONObject();
		if(StringUtils.isEmpty(pledge_info)) {
			Object[] array = data.keySet().toArray();
			for (int i = 0; i < array.length; i++) {
				String key = String.valueOf(array[i]);
				if(!columnList.contains(key)) {
					continue;
				}
				newObj.put(key, data.get(key) == null ? "" : data.get(key));
			}
		}
		JSONArray oldArray = JSON.parseArray(pledge_info);
		for (int i = 0; i < oldArray.size(); i++) {
			JSONObject oldObj = oldArray.getJSONObject(i);
			String id = oldObj.getString("id");
			if(id.equals(data.get("id"))) {
				newArray.add(newObj);
				continue;
			}
			newArray.add(oldObj);
		}
		columnList.clear();
		columnList.add("pledge_info");
		data.clear();
		data.put("pledge_info", newArray.toJSONString());
		String sqlPrefix = "INSERT INTO "+tidbTableName;
		String sql = getSqlStr(columnList, sqlPrefix, data);
		if(StringUtils.isEmpty(sql)) {
			return;
		}
		tidbPreparedStatement = tidbConnection.prepareStatement(sql);
		tidbPreparedStatement.executeUpdate();
	}
	
	
	private void processOrderSparePartImg(List<String> columnList,Map<String, String> data) throws SQLException {
		String order_no = "";
		String spare_part_id = data.get("spare_part_id");
		StringBuilder sb = new StringBuilder("select order_no from ").append(mysqlTableName).append(" where id = ").append(spare_part_id);
		mysqlPreparedStatement = mysqlConnection.prepareStatement(sb.toString());
		ResultSet rst = mysqlPreparedStatement.executeQuery();
		if(rst.next()) {
			order_no = rst.getString("order_no");
		}
		if(StringUtils.isEmpty(order_no)) {
			return;
		}
		data.put("order_no", order_no);
		String sqlType = "INSERT INTO "+tidbImgTableName;
		String sql = getSqlStr(columnList,sqlType,data);
		logger.info("往mysql中插入数据，sql = "+sql);
		if(StringUtils.isEmpty(sql)) {
			return;
		}
		tidbPreparedStatement = tidbConnection.prepareStatement(sql);
		tidbPreparedStatement.executeUpdate();
	}
	
	private String getSqlStr(List<String> columnList,String sqlPrefix,Map<String, String> data) {
		StringBuilder sb = new StringBuilder(sqlPrefix);
		Object[] array = data.keySet().toArray();
		sb.append("(");
		StringBuilder csb = new StringBuilder();
		StringBuilder vsb = new StringBuilder();
		StringBuilder updateSb = new StringBuilder();
		for (int i = 0; i < array.length; i++) {
			String key = String.valueOf(array[i]);
			if(!columnList.contains(key)) {
				continue;
			}
			csb.append(key).append(",");
			vsb.append("'").append(StringUtils.isEmpty(data.get(key)) ? null : data.get(key)).append("',");
			updateSb.append(key).append("='").append(StringUtils.isEmpty(data.get(key)) ? null : data.get(key)).append("',");
		}
		String csbStr = csb.toString();
		String vsbStr = vsb.toString();
		String updateSbStr = updateSb.toString();
		if(!csbStr.endsWith(",") || !vsbStr.endsWith(",") || !updateSbStr.endsWith(",")) {
			return null;
		}
		csbStr = csbStr.substring(0,csbStr.length() -1);
		vsbStr = vsbStr.substring(0,vsbStr.length() -1);
		updateSbStr = updateSbStr.substring(0,updateSbStr.length() -1);
		sb.append(csbStr).append(") VALUES(").append(vsbStr).append(")");
		sb.append("ON DUPLICATE KEY UPDATE ").append(updateSbStr);
		return sb.toString();
	}

	@Override
	public void close() throws Exception {
		logger.info("mysql 连接关闭");
		if (tidbPreparedStatement != null) { 
			tidbPreparedStatement.close();
		} 
		if (tidbConnection != null) { 
			tidbConnection.close(); 
		}
		if (mysqlPreparedStatement != null) { 
			mysqlPreparedStatement.close();
		} 
		if (mysqlConnection != null) { 
			mysqlConnection.close(); 
		}
	}


}
