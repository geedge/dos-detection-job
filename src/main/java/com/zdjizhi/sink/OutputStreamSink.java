package com.zdjizhi.sink;

import com.zdjizhi.common.CommonConfig;
import com.zdjizhi.common.DosEventLog;
import com.zdjizhi.common.DosMetricsLog;
import com.zdjizhi.common.DosSketchLog;
import com.zdjizhi.etl.DosDetection;
import com.zdjizhi.etl.EtlProcessFunction;
import com.zdjizhi.etl.ParseSketchLog;
import com.zdjizhi.utils.FlinkEnvironmentUtils;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author 94976
 */
public class OutputStreamSink {
    private static final Logger logger = LoggerFactory.getLogger(OutputStreamSink.class);

    public static OutputTag<DosMetricsLog> outputTag = new OutputTag<DosMetricsLog>("traffic server ip metrics"){};

    public static void finalOutputSink(){
        try {
            SingleOutputStreamOperator<DosSketchLog> middleStream = getMiddleStream();
            DosEventSink.dosEventOutputSink(getEventSinkStream(middleStream));
            TrafficServerIpMetricsSink.sideOutputMetricsSink(middleStream);
            FlinkEnvironmentUtils.streamExeEnv.execute(CommonConfig.STREAM_EXECUTION_JOB_NAME);
        } catch (Exception e) {
            logger.error("任务启动失败 {}",e);
        }
    }

    private static SingleOutputStreamOperator<DosEventLog> getEventSinkStream(SingleOutputStreamOperator<DosSketchLog> middleStream){
        return middleStream.map(new DosDetection()).setParallelism(CommonConfig.FLINK_DETECTION_MAP_PARALLELISM);
    }

    private static SingleOutputStreamOperator<DosSketchLog> getMiddleStream(){
        return ParseSketchLog.getSketchSource()
                .keyBy(new KeysSelector())
                .window(TumblingEventTimeWindows.of(Time.seconds(CommonConfig.FLINK_WINDOW_MAX_TIME)))
                .process(new EtlProcessFunction())
                .setParallelism(CommonConfig.FLINK_FIRST_AGG_PARALLELISM);
    }

    private static class KeysSelector implements KeySelector<DosSketchLog, Tuple2<String, String>>{
        @Override
        public Tuple2<String, String> getKey(DosSketchLog dosSketchLog){
            return Tuple2.of(
                    dosSketchLog.getAttack_type(),
                    dosSketchLog.getDestination_ip());
        }
    }

}
