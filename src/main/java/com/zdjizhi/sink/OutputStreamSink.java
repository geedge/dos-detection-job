package com.zdjizhi.sink;

import com.zdjizhi.common.CommonConfig;
import com.zdjizhi.common.DosEventLog;
import com.zdjizhi.common.DosMetricsLog;
import com.zdjizhi.common.DosSketchLog;
import com.zdjizhi.etl.EtlProcessFunction;
import com.zdjizhi.etl.DosDetection;
import com.zdjizhi.etl.ParseSketchLog;
import com.zdjizhi.source.BaselineSource;
import com.zdjizhi.utils.FlinkEnvironmentUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author 94976
 */
public class OutputStreamSink {
    private static final Logger logger = LoggerFactory.getLogger(OutputStreamSink.class);

    public static OutputTag<DosMetricsLog> outputTag = new OutputTag<DosMetricsLog>("traffic server ip metrics"){};

    public static MapStateDescriptor<String, Map<String, Map<String, List<Integer>>>> descriptor = new MapStateDescriptor<>("boradcast-state",
            Types.STRING,
            new MapTypeInfo<>(String.class, new MapTypeInfo<>(String.class, (Class<List<Integer>>) (Class<?>) List.class).getTypeClass()));

    public static void finalOutputSink(){
        try {
            SingleOutputStreamOperator<DosSketchLog> middleStream = getMiddleStream();
            SingleOutputStreamOperator<DosEventLog> dosEventLogOutputStream = getOutputSinkStream(middleStream);
            DosEventSink.dosEventOutputSink(dosEventLogOutputStream);
            TrafficServerIpMetricsSink.sideOutputMetricsSink(middleStream);
            FlinkEnvironmentUtils.streamExeEnv.execute(CommonConfig.STREAM_EXECUTION_JOB_NAME);
        } catch (Exception e) {
            logger.error("");
        }
    }

    public static void main(String[] args) throws Exception {
        SingleOutputStreamOperator<DosSketchLog> middleStream = getMiddleStream();
        SingleOutputStreamOperator<DosEventLog> dosEventLogOutputStream = getOutputSinkStream(middleStream);
        DosEventSink.dosEventOutputSink(dosEventLogOutputStream);
        TrafficServerIpMetricsSink.sideOutputMetricsSink(middleStream);
        dosEventLogOutputStream.print();
        FlinkEnvironmentUtils.streamExeEnv.execute();
    }

    private static SingleOutputStreamOperator<DosEventLog> getOutputSinkStream(SingleOutputStreamOperator<DosSketchLog> middleStream){

        BroadcastStream<Map<String, Map<String,List<Integer>>>> broadcast = FlinkEnvironmentUtils.streamExeEnv
                .addSource(new BaselineSource())
                .broadcast(descriptor);
        logger.info("广播变量加载成功！！");

        return middleStream.keyBy(new SecondKeySelector())
//                .window(TumblingEventTimeWindows.of(Time.seconds(CommonConfig.FLINK_WINDOW_MAX_TIME)))
                .reduce(new SecondReduceFunc())
                .connect(broadcast)
                .process(new DosDetection());
    }

    private static SingleOutputStreamOperator<DosSketchLog> getMiddleStream(){
        return ParseSketchLog.getSketchSource()
                .keyBy(new FirstKeySelector())
                .window(TumblingEventTimeWindows.of(Time.seconds(CommonConfig.FLINK_WINDOW_MAX_TIME)))
                .process(new EtlProcessFunction());
    }

    private static String groupUniqSourceIp(String sourceIp1,String sourceIp2){
        HashSet<String> sourceIpSet = new HashSet<>();
        Collections.addAll(sourceIpSet, (sourceIp1 + "," + sourceIp2).split(","));
        if (sourceIpSet.size() > CommonConfig.SOURCE_IP_LIST_LIMIT){
            return StringUtils.join(takeUniqLimit(sourceIpSet,CommonConfig.SOURCE_IP_LIST_LIMIT),",");
        }
        return StringUtils.join(sourceIpSet,",");
    }

    private static<T> Collection<T> takeUniqLimit(Collection<T> collection, int limit){
        int i =0;
        Collection<T> newSet = new HashSet<>();
        for (T t:collection){
            if (i < limit){
                newSet.add(t);
                i += 1;
            }
        }
        return newSet;
    }

    private static class FirstKeySelector implements KeySelector<DosSketchLog, Tuple4<String, String, String, String>>{
        @Override
        public Tuple4<String, String, String, String> getKey(DosSketchLog dosSketchLog) throws Exception {
            return Tuple4.of(
                    dosSketchLog.getCommon_sled_ip(),
                    dosSketchLog.getCommon_data_center(),
                    dosSketchLog.getAttack_type(),
                    dosSketchLog.getDestination_ip());
        }
    }

    private static class SecondKeySelector implements KeySelector<DosSketchLog, Tuple2<String, String>> {
        @Override
        public Tuple2<String, String> getKey(DosSketchLog dosSketchLog) throws Exception {
            return Tuple2.of(
                    dosSketchLog.getAttack_type(),
                    dosSketchLog.getDestination_ip());
        }
    }

    private static class SecondReduceFunc implements ReduceFunction<DosSketchLog> {
        @Override
        public DosSketchLog reduce(DosSketchLog value1, DosSketchLog value2) throws Exception {
            value1.setSketch_sessions((value1.getSketch_sessions()+value2.getSketch_sessions())/2);
            value1.setSketch_bytes((value1.getSketch_bytes()+value2.getSketch_bytes())/2);
            value1.setSketch_packets((value1.getSketch_packets()+value2.getSketch_packets())/2);
            value1.setSource_ip(groupUniqSourceIp(value1.getSource_ip(),value2.getSource_ip()));
            return value1;
        }
    }

}
