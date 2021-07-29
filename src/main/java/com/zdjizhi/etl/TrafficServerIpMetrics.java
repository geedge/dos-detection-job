package com.zdjizhi.etl;

import com.zdjizhi.common.CommonConfig;
import com.zdjizhi.common.DosMetricsLog;
import com.zdjizhi.common.DosSketchLog;
import org.apache.flink.api.java.tuple.Tuple4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class TrafficServerIpMetrics {

    private static final Logger logger = LoggerFactory.getLogger(TrafficServerIpMetrics.class);

    static DosMetricsLog getOutputMetric(Tuple4<String, String, String, String> keys, DosSketchLog midResuleLog) {
        DosMetricsLog dosMetricsLog = new DosMetricsLog();
        dosMetricsLog.setSketch_start_time(timeFloor(System.currentTimeMillis()/1000));
        dosMetricsLog.setCommon_sled_ip(keys.f0);
        dosMetricsLog.setCommon_data_center(keys.f1);
        dosMetricsLog.setDestination_ip(keys.f2);
        dosMetricsLog.setAttack_type(keys.f3);
        dosMetricsLog.setSession_rate(midResuleLog.getSketch_sessions());
        dosMetricsLog.setPacket_rate(midResuleLog.getSketch_packets());
        dosMetricsLog.setBit_rate(midResuleLog.getSketch_bytes());
        logger.debug("metric 结果已加载：{}",dosMetricsLog.toString());
        return dosMetricsLog;
    }

    private static long timeFloor(long sketchStartTime){
        return sketchStartTime / CommonConfig.FLINK_WINDOW_MAX_TIME * CommonConfig.FLINK_WINDOW_MAX_TIME;
    }

}
