package com.bkjf.flink_example.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import com.bkjf.flink_example.kafka.KafkaBinLogEvent;

public class MySQLSink extends RichSinkFunction<KafkaBinLogEvent>{
	private static final long serialVersionUID = 1L;
	private Connection connection;
	private PreparedStatement preparedStatement;
	
	@Override
	public void invoke(KafkaBinLogEvent bean) throws Exception {
		try {
			System.out.println("====================>执行到这1");
			ParameterTool mysqlParameterTool = ParameterTool.fromPropertiesFile(MySQLSink.class.getResourceAsStream("/mysql.properties"));
			System.out.println("====================>执行到这2"+mysqlParameterTool.get("drivername"));
			Class.forName(mysqlParameterTool.get("drivername"));
			connection = DriverManager.getConnection(mysqlParameterTool.get("dburl"), mysqlParameterTool.get("username"), mysqlParameterTool.get("password"));
			String sql = "";
			if("INSERT".equals(bean.getOperType()) || "UPDATE".equals(bean.getOperType())) {
				String sqlType = "replace into report.xfl";
				Object[] array = bean.getData().getAfter().keySet().toArray();
				sql = getSqlStr(sqlType,array);
				if(StringUtils.isEmpty(sql)) {
					return;
				}
				preparedStatement = connection.prepareStatement(sql);
				for (int i = 0; i < array.length; i++) {
					preparedStatement.setString((i+1), bean.getData().getAfter().get(array[i]));
				}
				preparedStatement.executeUpdate();
			}else if("DELETE".equals(bean.getOperType())) {
				//TODO
			}
		}finally {
			if (preparedStatement != null) { 
				preparedStatement.close();
			} 
			if (connection != null) { 
				connection.close(); 
			}
		}
	}
	
	private String getSqlStr(String sqlType,Object[] array) {
		StringBuilder sb = new StringBuilder(sqlType);
		sb.append("(");
		StringBuilder csb = new StringBuilder();
		StringBuilder vsb = new StringBuilder();
		for (int i = 0; i < array.length; i++) {
			csb.append(array[i]).append(",");
			vsb.append("?").append(",");
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
}
