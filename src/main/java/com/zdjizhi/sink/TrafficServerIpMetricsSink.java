package com.zdjizhi.sink;

import com.zdjizhi.common.CommonConfig;
import com.zdjizhi.common.DosMetricsLog;
import com.zdjizhi.common.DosSketchLog;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.KafkaUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import static com.zdjizhi.sink.OutputStreamSink.outputTag;

class TrafficServerIpMetricsSink {

    static void sideOutputMetricsSink(SingleOutputStreamOperator<DosSketchLog> outputStream){
        DataStream<DosMetricsLog> sideOutput = outputStream.getSideOutput(outputTag);
        sideOutput.map(JsonMapper::toJsonString).addSink(KafkaUtils.getKafkaSink(CommonConfig.KAFKA_OUTPUT_METRIC_TOPIC_NAME))
                .setParallelism(CommonConfig.KAFKA_OUTPUT_METRIC_PARALLELISM);
    }

}
