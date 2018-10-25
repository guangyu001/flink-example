package com.bkjf.flink_example.kafka;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bkjf.flink_example.mysql.MySQLSink;

public class FlinkKafkaToMysqlDemo {
	public static void main(String[] args) throws Exception {
		System.out.println("123");
		ParameterTool parameterTool = ParameterTool.fromPropertiesFile(FlinkKafkaToMysqlDemo.class.getResourceAsStream("/kafka.properties"));
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
				JSONObject obj = (JSONObject) JSON.parse(value);
				KafkaBinLogEvent bean = JSONObject.toJavaObject(obj, KafkaBinLogEvent.class);
				return bean;
			}
		});
		dataStream2.print();
		dataStream2.addSink(new MySQLSink());
		env.execute("kafka message save to mysql");
	}
}
