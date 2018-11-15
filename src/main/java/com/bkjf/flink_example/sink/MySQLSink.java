package com.bkjf.flink_example.sink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bkjf.flink_example.bean.KafkaBinLogEvent;


public class MySQLSink extends RichSinkFunction<KafkaBinLogEvent>{
	private Logger logger = LoggerFactory.getLogger(MySQLSink.class);
	private static final long serialVersionUID = 1L;
	private static final String DRIVER_NAME = "driverName";
	private static final String TIDB_USER_NAME = "tidbUserName";
	private static final String TIDB_PASSWORD = "tidbPassword";
	private static final String TIDB_DBURL = "tidbDBUrl";
	private static final String TIDB_TABLE_NAME = "tidbTableName";
	
	private Connection tidbConnection = null;
	private PreparedStatement tidbPreparedStatement = null;
	private String tidbTableName = null;
	
	private Map<String, List<String>> columnProcessMap;
	private Map<String, String> columnMap;
	private ParameterTool dbParameterTool;
	
	public MySQLSink(Map<String, List<String>> columnProcessMap,Map<String, String> columnMap,ParameterTool mysqlParameterTool) {
		this.columnProcessMap = columnProcessMap;
		this.columnMap = columnMap;
		this.dbParameterTool = mysqlParameterTool;
	}
	
	@Override
	public void open(Configuration parameters) throws Exception {
		logger.info("连接db，dbUrl = "+TIDB_DBURL);
		Class.forName(dbParameterTool.get(DRIVER_NAME));
		tidbConnection = DriverManager.getConnection(dbParameterTool.get(TIDB_DBURL), dbParameterTool.get(TIDB_USER_NAME), dbParameterTool.get(TIDB_PASSWORD));
		tidbTableName = dbParameterTool.get(TIDB_TABLE_NAME);
	}
	
	@Override
	public void invoke(KafkaBinLogEvent bean) throws Exception {
		String tableName = bean.getTableName();
		List<String> columnList = columnProcessMap.get(tableName);
		if(columnList == null || columnList.size() == 0) {
			return;
		}
		String sql = "";
		try {
			if("INSERT".equals(bean.getOperType()) || "UPDATE".equals(bean.getOperType())) {
				String sqlType = "INSERT INTO "+tidbTableName;
				sql = getSqlStr(bean.getDbName(),tableName,columnList,sqlType,bean.getData().getAfter());
				if(StringUtils.isEmpty(sql)) {
					return;
				}
				logger.info("往聚合db中插入数据，sql = "+sql);
				tidbPreparedStatement = tidbConnection.prepareStatement(sql);
				tidbPreparedStatement.executeUpdate();
			}else if("DELETE".equals(bean.getOperType())) {
				//TODO
			}
		}catch (Exception e) {
			logger.error("执行出现异常,sql = "+sql+" tableName = "+tableName,e);
		}
	}
	
	private String getSqlStr(String dbName,String tableName,List<String> columnList,String sqlPrefix,Map<String, String> data) {
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
			if("order_no".equals(key) && StringUtils.isEmpty(data.get(key))) {
				logger.error("order_no为空，dbName = "+dbName+"  tableName = "+tableName);
				continue;
			}
			String cloumn = columnMap.get(tableName+"_"+key) == null ? key : columnMap.get(tableName+"_"+key);
			String value = getValue(data.get(key));
			csb.append(cloumn).append(",");
			vsb.append(value).append(",");
			updateSb.append(cloumn).append(" = ").append(value).append(",");
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
	
	private String getValue(String str) {
		if(StringUtils.isEmpty(str)) {
			return null;
		}
		if(str.endsWith("'")) {
			return "'"+str;
		}
		if("null".equals(str)) {
			return null;
		}
		return "'"+str+"'";
	}

	@Override
	public void close() throws Exception {
		logger.info("db 连接关闭");
		if (tidbPreparedStatement != null) { 
			tidbPreparedStatement.close();
		} 
		if (tidbConnection != null) { 
			tidbConnection.close(); 
		}
	}


}
