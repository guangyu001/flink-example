package com.bkjf.flink_example.main;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
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
import com.bkjf.flink_example.sink.ReportCountSink;
import com.bkjf.flink_example.utils.ConfigUtils;

public class StreamReportMain{
	private static Logger logger = LoggerFactory.getLogger(BaseMain.class);
	private static final String SYSTEM_CONFIG_NAME = "/system.properties";
	private static final String KAFKA_CONFIG_NAME = "/kafka.properties";
	private static final String DB_CONFIG_NAME = "/db.properties";
	private static ParameterTool sysParameterTool;
	private static ParameterTool parameterTool;
	private static ParameterTool mysqlParameterTool;
	private static Map<String, List<String>> columnProcessConfig;
	private static Map<String, String> columnMapConfig;

	public static void main(String[] args) throws Exception {
		sysParameterTool = ParameterTool.fromPropertiesFile(FlinkKafkaToMysqlDemo.class.getResourceAsStream(SYSTEM_CONFIG_NAME));
		parameterTool = ParameterTool.fromPropertiesFile(FlinkKafkaToMysqlDemo.class.getResourceAsStream(KAFKA_CONFIG_NAME));
		mysqlParameterTool = ParameterTool.fromPropertiesFile(MySQLSink.class.getResourceAsStream(DB_CONFIG_NAME));
		columnProcessConfig = ConfigUtils.getColumnProcessConfig(sysParameterTool);
		columnMapConfig = ConfigUtils.getColumnMapConfig(sysParameterTool);
		logger.info("开始执行flink程序,参数如下：");
		logger.info("columnProcessConfig = "+columnProcessConfig);
		logger.info("columnMapConfig = "+columnMapConfig);
		startRun();
	}
	private static AtomicLong count = new AtomicLong();
	private static void startRun() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		//关闭日志
		env.getConfig().disableSysoutLogging();
		//并发执行
		env.setMaxParallelism(4);
		//出现错误重启的方式重试3次
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,Time.of(10, TimeUnit.SECONDS)));
		//检查间隔
		env.enableCheckpointing(5000);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		// 设置参数
		env.getConfig().setGlobalJobParameters(parameterTool); 
		Properties properties = parameterTool.getProperties();
		properties.remove("lft_report_topic");
		DataStream<String> dataStream = env.addSource(new FlinkKafkaConsumer011<>(parameterTool.getRequired("lft_report_topic"), new SimpleStringSchema(), properties)).setParallelism(2);
		DataStream<KafkaBinLogEvent> dataStream2 = dataStream.map(new MapFunction<String, KafkaBinLogEvent>() {
			private static final long serialVersionUID = 1L;

			@Override
			public KafkaBinLogEvent map(String value) throws Exception {
				JSONObject obj = (JSONObject) JSON.parse(value);
				KafkaBinLogEvent bean = JSONObject.toJavaObject(obj, KafkaBinLogEvent.class);
				count.addAndGet(1);
				System.out.println(count.longValue());
				return bean;
			}
		}).setParallelism(1);
		dataStream2.addSink(new ReportCountSink(columnProcessConfig, columnMapConfig, mysqlParameterTool)).setParallelism(4);
		System.out.println(env.getExecutionPlan());
		env.execute("lft stream report");
	}
}
