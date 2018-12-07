package com.bkjf.flink_example.main;

import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bkjf.flink_example.bean.KafkaBinLogEvent;
import com.bkjf.flink_example.sink.ReportCountSink;

public class StreamReportMain extends BaseMain{

	public StreamReportMain() throws IOException {
		super();
	}

	public static void main(String[] args) throws Exception {
		new StreamReportMain().startRun();
	}
	
	private void startRun() throws Exception {
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
		Properties properties = parameterTool.getProperties();
		properties.remove("lft_report_topic");
		DataStream<String> dataStream = env.addSource(new FlinkKafkaConsumer011<>(parameterTool.getRequired("lft_report_topic"), new SimpleStringSchema(), properties));
		DataStream<KafkaBinLogEvent> dataStream2 = dataStream.map(new MapFunction<String, KafkaBinLogEvent>() {
			private static final long serialVersionUID = 1L;
			@Override
			public KafkaBinLogEvent map(String value) throws Exception {
				JSONObject obj = (JSONObject) JSON.parse(value);
				KafkaBinLogEvent bean = JSONObject.toJavaObject(obj, KafkaBinLogEvent.class);
				return bean;
			}
		});
		dataStream2.addSink(new ReportCountSink(columnProcessConfig, columnMapConfig, mysqlParameterTool));
		System.out.println(env.getExecutionPlan());
		env.execute("kafka message save to mysql");
	}
}
