package com.bkjf.flink_example.main;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bkjf.flink_example.bean.KafkaBinLogEvent;
import com.bkjf.flink_example.sink.MySQLSink;
import com.bkjf.flink_example.utils.ConfigUtils;

public class FlinkKafkaToMysqlDemo {
	private static Logger logger = LoggerFactory.getLogger(FlinkKafkaToMysqlDemo.class);
	private static Map<String, String> keyByMap = new HashMap<>();
	
	private static final String IS_KEYBY_TABLE_NAME = "isKeyByTableName";
	private static final String SYSTEM_CONFIG_NAME = "/system.properties";
	private static final String KAFKA_CONFIG_NAME = "/kafka.properties";
	private static final String DB_CONFIG_NAME = "/db.properties";
	private static AtomicLong count = new AtomicLong();
	public static void main(String[] args) throws Exception {
		ParameterTool sysParameterTool = ParameterTool.fromPropertiesFile(FlinkKafkaToMysqlDemo.class.getResourceAsStream(SYSTEM_CONFIG_NAME));
		ParameterTool parameterTool = ParameterTool.fromPropertiesFile(FlinkKafkaToMysqlDemo.class.getResourceAsStream(KAFKA_CONFIG_NAME));
		ParameterTool mysqlParameterTool = ParameterTool.fromPropertiesFile(MySQLSink.class.getResourceAsStream(DB_CONFIG_NAME));
		String str = sysParameterTool.get(IS_KEYBY_TABLE_NAME);
		if(!StringUtils.isEmpty(str)) {
			//tableName.column
			String[] strs = str.split("\\.");
			if(strs.length == 2) {
				keyByMap.put(strs[0], strs[1]);
			}
		}
		Map<String, List<String>> columnProcessConfig = ConfigUtils.getColumnProcessConfig(sysParameterTool);
		Map<String, String> columnMapConfig = ConfigUtils.getColumnMapConfig(sysParameterTool);
		logger.info("开始执行flink程序,参数如下：");
		logger.info("keyByMap = "+keyByMap);
		logger.info("columnProcessConfig = "+columnProcessConfig);
		logger.info("columnMapConfig = "+columnMapConfig);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		//关闭日志
		env.getConfig().disableSysoutLogging();
		//并发执行
		env.setParallelism(4);
		//出现错误重启的方式重试3次
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,Time.of(10, TimeUnit.SECONDS)));
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
				JSONObject obj = (JSONObject) JSON.parse(value);
				count.addAndGet(1);
				System.out.println(count.intValue());
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
					return value.getId()+"";
				}
				return keyByName;
			}
		}).addSink(new MySQLSink(columnProcessConfig,columnMapConfig,mysqlParameterTool));
		System.out.println(env.getExecutionPlan());
		env.execute("kafka message save to mysql");
	}
	
	
}
