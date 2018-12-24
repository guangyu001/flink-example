package com.bkjf.flink_example.main;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bkjf.flink_example.bean.KafkaBinLogEvent;
import com.bkjf.flink_example.sink.MySQLSink;
import com.bkjf.flink_example.sink.UserCountSink;
import org.apache.flink.api.common.functions.FilterFunction;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @Author: guang yu
 * @Description:
 * @Date: Create in 19:05
 * @Modified By: your name
 */

public class StreamUserCountMain {
    private static Logger logger = LoggerFactory.getLogger(StreamUserCountMain.class);
    private static Map<String, String> keyByMap = new HashMap<>();
    private static final String COLUMN_PROCESS = "columnprocess";
    private static final String COLUMN_MAP = "columnmap";
    private static final String IS_KEYBY_TABLE_NAME = "isKeyByTableName";
    private static final String SYSTEM_CONFIG_NAME = "/system.properties";
    private static final String KAFKA_CONFIG_NAME = "/kafka.properties";
    private static final String DB_CONFIG_NAME = "/db.properties";
    private static Map<String, List<String>> columnProcessConfig;
    private static Map<String, String> columnMapConfig;
    public static void main(String[] args) throws Exception {
        ParameterTool sysParameterTool = ParameterTool.fromPropertiesFile(StreamUserCountMain.class.getResourceAsStream(SYSTEM_CONFIG_NAME));
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(StreamUserCountMain.class.getResourceAsStream(KAFKA_CONFIG_NAME));
        ParameterTool mysqlParameterTool = ParameterTool.fromPropertiesFile(MySQLSink.class.getResourceAsStream(DB_CONFIG_NAME));
        String str = sysParameterTool.get(IS_KEYBY_TABLE_NAME);
        //Map<String, List<String>> columnProcessConfig = getColumnProcessConfig(sysParameterTool);
        // Map<String, String> columnMapConfig = getColumnMapConfig(sysParameterTool);


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //关闭日志
        env.getConfig().disableSysoutLogging();
        //并发执行
        env.setParallelism(4);
        //出现错误重启的方式重试3次
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));
        //检查间隔
        env.enableCheckpointing(5000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        DataStream<String> dataStream = env.addSource(new FlinkKafkaConsumer011<>(parameterTool.getRequired("lft_user_count_topic"), new SimpleStringSchema(), parameterTool.getProperties()));

        DataStream<KafkaBinLogEvent> dataStream2 = dataStream.map(new MapFunction<String, KafkaBinLogEvent>() {
            private static final long serialVersionUID = 1L;
            @Override
            public KafkaBinLogEvent map(String value) throws Exception {
                JSONObject obj = (JSONObject) JSON.parse(value);
                //count.addAndGet(1);
                //System.out.println(count.intValue());
                KafkaBinLogEvent bean = JSONObject.toJavaObject(obj, KafkaBinLogEvent.class);
                return bean;
            }
        }).filter(new FilterFunction<KafkaBinLogEvent>() {
            @Override
            public boolean filter(KafkaBinLogEvent event) throws Exception {
                if("INSERT".equalsIgnoreCase(event.getOperType()) || "DELETE".equalsIgnoreCase(event.getOperType())){
                    return true;
                }
                return false;
            }
        });
        dataStream2.addSink(new UserCountSink(columnProcessConfig, columnMapConfig, mysqlParameterTool));

        System.out.println(env.getExecutionPlan());
        env.execute("actual lft_report_real_all_user_cnt");
    }
}
