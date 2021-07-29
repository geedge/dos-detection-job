package com.zdjizhi.utils;

import com.zdjizhi.common.CommonConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class KafkaUtils {

    private static Properties getKafkaSinkProperty(){
        Properties propertiesproducer = new Properties();
        propertiesproducer.setProperty("bootstrap.servers", CommonConfig.KAFKA_OUTPUT_BOOTSTRAP_SERVERS);

        return propertiesproducer;
    }

    public static FlinkKafkaProducer<String> getKafkaSink(String topic){
        return new FlinkKafkaProducer<String>(
                topic,
                new SimpleStringSchema(),
                getKafkaSinkProperty()
        );
    }

}
