package com.zdjizhi.etl;

import com.zdjizhi.common.CommonConfig;
import com.zdjizhi.common.DosMetricsLog;
import com.zdjizhi.common.DosSketchLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class TrafficServerIpMetrics {

    private static final Logger logger = LoggerFactory.getLogger(TrafficServerIpMetrics.class);

    static DosMetricsLog getOutputMetric(DosSketchLog midResuleLog) {
        DosMetricsLog dosMetricsLog = new DosMetricsLog();
        dosMetricsLog.setSketch_start_time(timeFloor(System.currentTimeMillis()/1000));
        dosMetricsLog.setCommon_sled_ip(midResuleLog.getCommon_sled_ip());
        dosMetricsLog.setCommon_data_center(midResuleLog.getCommon_data_center());
        dosMetricsLog.setDestination_ip(midResuleLog.getDestination_ip());
        dosMetricsLog.setAttack_type(midResuleLog.getAttack_type());
        dosMetricsLog.setSession_rate(midResuleLog.getSketch_sessions());
        dosMetricsLog.setPacket_rate(midResuleLog.getSketch_packets());
        dosMetricsLog.setBit_rate(midResuleLog.getSketch_bytes());
        logger.info("metric 结果已加载：{}",dosMetricsLog.toString());
        return dosMetricsLog;
    }

    private static long timeFloor(long sketchStartTime){
        return sketchStartTime / CommonConfig.FLINK_WINDOW_MAX_TIME * CommonConfig.FLINK_WINDOW_MAX_TIME;
    }

}
