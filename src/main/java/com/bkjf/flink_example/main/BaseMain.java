package com.bkjf.flink_example.main;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bkjf.flink_example.sink.MySQLSink;
import com.bkjf.flink_example.utils.ConfigUtils;

public class BaseMain {
	public Logger logger = LoggerFactory.getLogger(BaseMain.class);
	private static final String SYSTEM_CONFIG_NAME = "/system.properties";
	private static final String KAFKA_CONFIG_NAME = "/kafka.properties";
	private static final String DB_CONFIG_NAME = "/db.properties";
	public ParameterTool sysParameterTool;
	public ParameterTool parameterTool;
	public ParameterTool mysqlParameterTool;
	public Map<String, List<String>> columnProcessConfig;
	public Map<String, String> columnMapConfig;
	public BaseMain() throws IOException {
		sysParameterTool = ParameterTool.fromPropertiesFile(FlinkKafkaToMysqlDemo.class.getResourceAsStream(SYSTEM_CONFIG_NAME));
		parameterTool = ParameterTool.fromPropertiesFile(FlinkKafkaToMysqlDemo.class.getResourceAsStream(KAFKA_CONFIG_NAME));
		mysqlParameterTool = ParameterTool.fromPropertiesFile(MySQLSink.class.getResourceAsStream(DB_CONFIG_NAME));
		columnProcessConfig = ConfigUtils.getColumnProcessConfig(sysParameterTool);
		columnMapConfig = ConfigUtils.getColumnMapConfig(sysParameterTool);
		logger.info("开始执行flink程序,参数如下：");
		logger.info("columnProcessConfig = "+columnProcessConfig);
		logger.info("columnMapConfig = "+columnMapConfig);
	}
}
