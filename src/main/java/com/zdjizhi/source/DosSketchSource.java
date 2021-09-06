package com.zdjizhi.source;

import com.zdjizhi.common.CommonConfig;
import com.zdjizhi.utils.FlinkEnvironmentUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author wlh
 */
public class DosSketchSource {

    private static StreamExecutionEnvironment streamExeEnv = FlinkEnvironmentUtils.streamExeEnv;

    public static DataStreamSource<String> createDosSketchSource(){
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", CommonConfig.KAFKA_INPUT_BOOTSTRAP_SERVERS);
        properties.setProperty("group.id", CommonConfig.KAFKA_GROUP_ID);
        properties.setProperty("security.protocol", "SASL_PLAINTEXT");
        properties.setProperty("sasl.mechanism", "PLAIN");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""+CommonConfig.SASL_JAAS_CONFIG_USER+"\" password=\""+CommonConfig.SASL_JAAS_CONFIG_PASSWORD+"\";");

        return streamExeEnv.addSource(new FlinkKafkaConsumer<String>(
                CommonConfig.KAFKA_INPUT_TOPIC_NAME,
                new SimpleStringSchema(), properties))
                .setParallelism(CommonConfig.KAFKA_INPUT_PARALLELISM);
    }
}
