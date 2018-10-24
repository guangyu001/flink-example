package com.bkjf.flink_example.kafka;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkKafkaDemo {
	private static final String KAFKA_BROKER = "spark-032131.lanxun.bkjk.cn:9092,spark-032132.lanxun.bkjk.cn:9092,spark-032133.lanxun.bkjk.cn:9092";
	private static final String ZK_SERVER = "spark-032131.lanxun.bkjk.cn:2181,spark-032132.lanxun.bkjk.cn:2181,spark-032133.lanxun.bkjk.cn:2181";
	public static final String GROUP_ID = "flink_test";
	public static final String TOPIC = "test";
	
	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		//关闭日志
		env.getConfig().disableSysoutLogging();
		//重试次数
		env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
		//检查间隔
		env.enableCheckpointing(5000);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		
		/*kafkaProps.setProperty("zookeeper.connect", ZK_SERVER);
		kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER);
		kafkaProps.setProperty("group.id", GROUP_ID);
		kafkaProps.setProperty("auto.offset.reset", "earliest");*/
		
	}
}
