package com.bkjf.flink_example.sink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bkjf.flink_example.bean.KafkaBinLogEvent;

public class BaseSink extends RichSinkFunction<KafkaBinLogEvent>{
	private static final long serialVersionUID = 1L;
	public Logger logger = LoggerFactory.getLogger(MySQLSink.class);
	private static final String DRIVER_NAME = "driverName";
	private static final String TIDB_USER_NAME = "tidbUserName";
	private static final String TIDB_PASSWORD = "tidbPassword";
	private static final String TIDB_DBURL = "tidbDBUrl";
	private static final String TIDB_TABLE_NAME = "tidbTableName";
	
	public Connection tidbConnection = null;
	public PreparedStatement tidbPreparedStatement = null;
	public String tidbTableName = null;
	
	public Map<String, List<String>> columnProcessMap;
	public Map<String, String> columnMap;
	private ParameterTool dbParameterTool;
	
	public BaseSink(Map<String, List<String>> columnProcessMap,Map<String, String> columnMap,ParameterTool mysqlParameterTool) {
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
