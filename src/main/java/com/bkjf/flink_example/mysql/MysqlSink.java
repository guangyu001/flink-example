package com.bkjf.flink_example.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bkjf.flink_example.kafka.KafkaBinLogEvent;

public class MysqlSink extends RichSinkFunction<KafkaBinLogEvent>{

	private Logger logger = LoggerFactory.getLogger(MysqlSink.class);
	private Connection connection = null;
	private PreparedStatement preparedStatement = null;
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	
	@Override
	public void open(Configuration parameters) throws Exception {
		logger.info("开始连接mysql");
		ParameterTool mysqlParameterTool = ParameterTool.fromPropertiesFile(MySQLSink.class.getResourceAsStream("/mysql.properties"));
		Class.forName(mysqlParameterTool.get("drivername"));
		connection = DriverManager.getConnection(mysqlParameterTool.get("dburl"), mysqlParameterTool.get("username"), mysqlParameterTool.get("password"));
		logger.info("mysqlUrl = "+mysqlParameterTool.get("dburl"));
	}
	
	@Override
	public void invoke(KafkaBinLogEvent bean) throws Exception {
		String sql = "";
		if("INSERT".equals(bean.getOperType()) || "UPDATE".equals(bean.getOperType())) {
			String sqlType = "replace into report.xfl";
			sql = getSqlStr(sqlType,bean.getData().getAfter());
			if(StringUtils.isEmpty(sql)) {
				return;
			}
			logger.info("往mysql中插入数据，sql = "+sql);
			preparedStatement = connection.prepareStatement(sql);
			preparedStatement.executeUpdate();
		}else if("DELETE".equals(bean.getOperType())) {
			//TODO
		}
	}
	
	private String getSqlStr(String sqlType,Map<String, String> data) {
		StringBuilder sb = new StringBuilder(sqlType);
		Object[] array = data.keySet().toArray();
		sb.append("(");
		StringBuilder csb = new StringBuilder();
		StringBuilder vsb = new StringBuilder();
		for (int i = 0; i < array.length; i++) {
			csb.append(array[i]).append(",");
			vsb.append("'").append(StringUtils.isEmpty(data.get(array[i])) ? null : data.get(array[i])).append("',");
		}
		String csbStr = csb.toString();
		String vsbStr = vsb.toString();
		if(!csbStr.endsWith(",") || !vsbStr.endsWith(",")) {
			return null;
		}
		csbStr = csbStr.substring(0,csbStr.length() -1);
		vsbStr = vsbStr.substring(0,vsbStr.length() -1);
		sb.append(csbStr).append(") values(").append(vsbStr).append(")");
		return sb.toString();
	}

	@Override
	public void close() throws Exception {
		logger.info("mysql 连接关闭");
		if (preparedStatement != null) { 
			preparedStatement.close();
		} 
		if (connection != null) { 
			connection.close(); 
		}
	}


}
