package com.bkjf.flink_example.main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bkjf.flink_example.bean.KafkaBinLogEvent;
import com.bkjf.flink_example.sink.MySQLSink;

public class FlinkKafkaToMysqlDemo {
	private static Map<String, String> keyByMap = new HashMap<>();
	private static final String IS_KEYBY_TABLE_NAME = "isKeyByTableName";
	private static final String SYSTEM_CONFIG_NAME = "/system.properties";
	private static final String KAFKA_CONFIG_NAME = "/kafka.properties";
	private static final String DB_CONFIG_NAME = "/db.properties";
	public static void main(String[] args) throws Exception {
		ParameterTool sysParameterTool = ParameterTool.fromPropertiesFile(FlinkKafkaToMysqlDemo.class.getResourceAsStream(SYSTEM_CONFIG_NAME));
		ParameterTool parameterTool = ParameterTool.fromPropertiesFile(FlinkKafkaToMysqlDemo.class.getResourceAsStream(KAFKA_CONFIG_NAME));
		ParameterTool mysqlParameterTool = ParameterTool.fromPropertiesFile(MySQLSink.class.getResourceAsStream(DB_CONFIG_NAME));
		String str = sysParameterTool.get(IS_KEYBY_TABLE_NAME);
		if(!StringUtils.isEmpty(str)) {
			String[] strs = str.split("#");
			if(strs.length == 2) {
				keyByMap.put(strs[0], strs[1]);
			}
		}
		Map<String, List<String>> systemConfig = getSystemConfig(sysParameterTool);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		//关闭日志
		env.getConfig().disableSysoutLogging();
		//重试次数
		env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000));
		//检查间隔
		env.enableCheckpointing(5000);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		// 设置参数
		env.getConfig().setGlobalJobParameters(parameterTool); 
		DataStream<String> dataStream = env.addSource(new FlinkKafkaConsumer011<>(parameterTool.getRequired("topic"), new SimpleStringSchema(), parameterTool.getProperties()));
		DataStream<KafkaBinLogEvent> dataStream2 = dataStream.map(new MapFunction<String, KafkaBinLogEvent>() {
			private static final long serialVersionUID = 1L;
			@Override
			public KafkaBinLogEvent map(String value) throws Exception {
				System.out.println("-------------------------------"+value);
				JSONObject obj = (JSONObject) JSON.parse(value);
				KafkaBinLogEvent bean = JSONObject.toJavaObject(obj, KafkaBinLogEvent.class);
				return bean;
			}
		});
		dataStream2.keyBy(new KeySelector<KafkaBinLogEvent, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public String getKey(KafkaBinLogEvent value) throws Exception {
				String keyByName = keyByMap.get(value.getTableName());
				if (StringUtils.isEmpty(keyByName)) {
					return "";
				}
				return keyByName;
			}
		}).addSink(new MySQLSink(systemConfig,mysqlParameterTool));
		env.execute("kafka message save to mysql");
	}
	
	private static Map<String, List<String>> getSystemConfig(ParameterTool sysParameterTool) throws IOException {
		Map<String, List<String>> configMap = new HashMap<>();
		Set<String> nameSet = sysParameterTool.getProperties().stringPropertyNames();
		for(String name : nameSet) {
			if(name.equals(IS_KEYBY_TABLE_NAME)) {
				continue;
			}
			String csStr = sysParameterTool.get(name);
			if(StringUtils.isEmpty(csStr)) {
				continue;
			}
			String[] cs = csStr.split(",");
			List<String> list = new ArrayList<>();
			for(String c : cs) {
				list.add(c);
			}
			configMap.put(name, list);
		}
		return configMap;
	}
}
