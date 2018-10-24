package com.bkjf.flink_example.kafka;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

public class RideCleansingToKafka {
	private static final String KAFKA_BROKER = "spark-032131.lanxun.bkjk.cn:9092,spark-032132.lanxun.bkjk.cn:9092,spark-032133.lanxun.bkjk.cn:9092";
	private static final String ZK_SERVER = "spark-032131.lanxun.bkjk.cn:2181,spark-032132.lanxun.bkjk.cn:2181,spark-032133.lanxun.bkjk.cn:2181";
	public static final String GROUP_ID = "flink_test";
	public static final String TOPIC = "test";
	
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.enableCheckpointing(1000);
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
		Properties kafkaProps = new Properties();
		kafkaProps.setProperty("zookeeper.connect", ZK_SERVER);
		kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER);
		kafkaProps.setProperty("group.id", GROUP_ID);
		kafkaProps.setProperty("auto.offset.reset", "earliest");
		
		DataStreamSource<String> addSource = env.addSource(new FlinkKafkaConsumer010<String>(TOPIC, new SimpleStringSchema(), kafkaProps));
		addSource.print();
		env.execute();
	}
}
