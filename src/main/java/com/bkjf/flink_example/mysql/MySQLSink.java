package com.bkjf.flink_example.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Map;

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
			ParameterTool mysqlParameterTool = ParameterTool.fromPropertiesFile(MySQLSink.class.getResourceAsStream("/mysql.properties"));
			Class.forName(mysqlParameterTool.get("drivername"));
			connection = DriverManager.getConnection(mysqlParameterTool.get("dburl"), mysqlParameterTool.get("username"), mysqlParameterTool.get("password"));
			String sql = "";
			if("INSERT".equals(bean.getOperType()) || "UPDATE".equals(bean.getOperType())) {
				String sqlType = "replace into report.xfl";
				sql = getSqlStr(sqlType,bean.getData().getAfter());
				if(StringUtils.isEmpty(sql)) {
					return;
				}
				System.out.println(sql);
				preparedStatement = connection.prepareStatement(sql);
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
	
	private String getSqlStr(String sqlType,Map<String, String> data) {
		StringBuilder sb = new StringBuilder(sqlType);
		Object[] array = data.keySet().toArray();
		sb.append("(");
		StringBuilder csb = new StringBuilder();
		StringBuilder vsb = new StringBuilder();
		for (int i = 0; i < array.length; i++) {
			csb.append(array[i]).append(",");
			vsb.append("'").append(data.get(array[i])).append("',");
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
