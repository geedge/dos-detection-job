package com.zdjizhi.etl;

import com.zdjizhi.common.CommonConfig;
import com.zdjizhi.common.DosSketchLog;
import com.zdjizhi.source.DosSketchSource;
import com.zdjizhi.utils.FlinkEnvironmentUtils;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;

public class ParseSketchLog {

    private static Logger logger = LoggerFactory.getLogger(ParseSketchLog.class);

    public static SingleOutputStreamOperator<DosSketchLog> getSketchSource(){
        return flatSketchSource();
    }

    private static SingleOutputStreamOperator<DosSketchLog> flatSketchSource(){
        return DosSketchSource.createDosSketchSourceByDatastream()
                .flatMap(new flatSketchLog())
                .assignTimestampsAndWatermarks(createWatermarkStrategy());
    }

    private static WatermarkStrategy<DosSketchLog> createWatermarkStrategy(){
        return WatermarkStrategy
                .<DosSketchLog>forBoundedOutOfOrderness(Duration.ofSeconds(CommonConfig.FLINK_WATERMARK_MAX_ORDERNESS))
                .withTimestampAssigner((event, timestamp) -> event.getSketch_start_time() * 1000);
    }

    private static class flatSketchLog implements FlatMapFunction<String, DosSketchLog> {
        @Override
        public void flatMap(String s, Collector<DosSketchLog> collector) throws Exception {
            try {
                if (StringUtil.isNotBlank(s)){
                    HashMap<String, Object> sketchSource = (HashMap<String, Object>) JsonMapper.fromJsonString(s, Object.class);
                    String commonSledIp = sketchSource.get("common_sled_ip").toString();
                    String commonDataCenter = sketchSource.get("common_data_center").toString();
                    long sketchStartTime = Long.parseLong(sketchSource.get("sketch_start_time").toString());
                    long sketchDuration = Long.parseLong(sketchSource.get("sketch_duration").toString());
                    String attackType = sketchSource.get("attack_type").toString();
                    ArrayList<HashMap<String, Object>> reportIpList = (ArrayList<HashMap<String, Object>>) sketchSource.get("report_ip_list");
                    for (HashMap<String, Object> obj : reportIpList) {
                        DosSketchLog dosSketchLog = new DosSketchLog();
                        dosSketchLog.setCommon_sled_ip(commonSledIp);
                        dosSketchLog.setCommon_data_center(commonDataCenter);
                        dosSketchLog.setSketch_start_time(sketchStartTime);
                        dosSketchLog.setSketch_duration(sketchDuration);
                        dosSketchLog.setAttack_type(attackType);
                        String sourceIp = obj.get("source_ip").toString();
                        String destinationIp = obj.get("destination_ip").toString();
                        long sketchSessions = Long.parseLong(obj.get("sketch_sessions").toString());
                        long sketchPackets = Long.parseLong(obj.get("sketch_packets").toString());
                        long sketchBytes = Long.parseLong(obj.get("sketch_bytes").toString());
                        dosSketchLog.setSource_ip(sourceIp);
                        dosSketchLog.setDestination_ip(destinationIp);
                        dosSketchLog.setSketch_sessions(sketchSessions/sketchDuration);
                        dosSketchLog.setSketch_packets(sketchPackets/sketchDuration);
                        dosSketchLog.setSketch_bytes(sketchBytes*8/sketchDuration);
                        collector.collect(dosSketchLog);
                        logger.info("数据解析成功：{}",dosSketchLog.toString());
                    }
                }
            } catch (Exception e) {
                logger.error("数据解析错误：{} \n{}",s,e);
            }
        }
    }


    public static void main(String[] args) throws Exception {
        flatSketchSource().print();
        FlinkEnvironmentUtils.streamExeEnv.execute();
    }

}
