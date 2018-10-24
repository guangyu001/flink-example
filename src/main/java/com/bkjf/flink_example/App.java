package com.bkjf.flink_example;

import org.apache.flink.api.java.utils.ParameterTool;

/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) {
		ParameterTool parameterTool = ParameterTool.fromArgs(args);
		
		if (parameterTool.getNumberOfParameters() < 5) {
			System.out.println("Missing parameters!\n" +
					"Usage: Kafka --input-topic <topic> --output-topic <topic> " +
					"--bootstrap.servers <kafka brokers> " +
					"--zookeeper.connect <zk quorum> --group.id <some id>");
			return;
		}
	}
}
