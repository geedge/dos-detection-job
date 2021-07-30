package com.zdjizhi.sink;

import com.zdjizhi.common.CommonConfig;
import com.zdjizhi.common.DosEventLog;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.KafkaUtils;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

class DosEventSink {

    static void dosEventOutputSink(SingleOutputStreamOperator<DosEventLog> dosEventLogOutputStream){
        dosEventLogOutputStream.map(JsonMapper::toJsonString).addSink(KafkaUtils.getKafkaSink(CommonConfig.KAFKA_OUTPUT_EVENT_TOPIC_NAME))
                .setParallelism(CommonConfig.KAFKA_OUTPUT_EVENT_PARALLELISM);
    }

}
