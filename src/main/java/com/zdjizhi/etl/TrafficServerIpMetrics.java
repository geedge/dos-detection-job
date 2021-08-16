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
        dosMetricsLog.setDestination_ip(midResuleLog.getDestination_ip());
        dosMetricsLog.setAttack_type(midResuleLog.getAttack_type());
        dosMetricsLog.setSession_rate(midResuleLog.getSketch_sessions());
        dosMetricsLog.setPacket_rate(midResuleLog.getSketch_packets());
        dosMetricsLog.setBit_rate(midResuleLog.getSketch_bytes());
        dosMetricsLog.setPartition_num(getPartitionNumByIp(midResuleLog.getDestination_ip()));
        logger.debug("metric 结果已加载：{}",dosMetricsLog.toString());
        return dosMetricsLog;
    }

    private static long timeFloor(long sketchStartTime){
        return sketchStartTime / CommonConfig.FLINK_WINDOW_MAX_TIME * CommonConfig.FLINK_WINDOW_MAX_TIME;
    }

    private static int getPartitionNumByIp(String destinationIp){
        return Math.abs(destinationIp.hashCode()) % CommonConfig.DESTINATION_IP_PARTITION_NUM;
    }

    public static void main(String[] args) {
        System.out.println(getPartitionNumByIp("146.177.223.43"));
        System.out.println("146.177.223.43".hashCode());
    }

}
