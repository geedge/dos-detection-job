package com.zdjizhi.etl;

import com.zdjizhi.common.CommonConfig;
import com.zdjizhi.common.DosDetectionThreshold;
import com.zdjizhi.common.DosEventLog;
import com.zdjizhi.common.DosSketchLog;
import com.zdjizhi.utils.HbaseUtils;
import com.zdjizhi.utils.IpUtils;
import com.zdjizhi.utils.SnowflakeId;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.TreeRangeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.*;

/**
 * @author wlh
 */
public class DosDetection extends RichMapFunction<DosSketchLog, DosEventLog> {

    private static final Logger logger = LoggerFactory.getLogger(DosDetection.class);
    private static Map<String, Map<String, Tuple2<ArrayList<Integer>, Integer>>> baselineMap;
    private final static int BASELINE_SIZE = 144;
    private final static NumberFormat PERCENT_INSTANCE = NumberFormat.getPercentInstance();
    private TreeRangeMap<IPAddress, Map<String, DosDetectionThreshold>> thresholdRangeMap;

    @Override
    public void open(Configuration parameters) {
        baselineMap = HbaseUtils.baselineMap;
        thresholdRangeMap = ParseStaticThreshold.createStaticThreshold();
        PERCENT_INSTANCE.setMinimumFractionDigits(2);
    }

    @Override
    public DosEventLog map(DosSketchLog value) {
        DosEventLog finalResult = null;
        try {
            String destinationIp = value.getDestination_ip();
            String attackType = value.getAttack_type();
            IPAddress destinationIpAddress = new IPAddressString(destinationIp).getAddress();
            Map<String, DosDetectionThreshold> thresholdMap = thresholdRangeMap.get(destinationIpAddress);
            logger.debug("当前判断IP：{}, 类型: {}", destinationIp, attackType);
            if (baselineMap.containsKey(destinationIp) && thresholdMap == null) {
                finalResult = getDosEventLogByBaseline(value, destinationIp, attackType);
            } else if (!baselineMap.containsKey(destinationIp) && thresholdMap != null) {
                finalResult = getDosEventLogByStaticThreshold(value,thresholdMap);
            }else if (baselineMap.containsKey(destinationIp) && thresholdMap != null){
                DosEventLog eventLogByBaseline = getDosEventLogByBaseline(value, destinationIp, attackType);
                DosEventLog eventLogByStaticThreshold = getDosEventLogByStaticThreshold(value, thresholdMap);
                finalResult = mergeFinalResult(eventLogByBaseline,eventLogByStaticThreshold);
            }else {
                logger.debug("未获取到当前server IP：{} 类型 {} 静态阈值 和 baseline", destinationIp, attackType);
            }
        } catch (Exception e) {
            logger.error("判定失败\n {} \n{}", value, e);
        }
        return finalResult;
    }

    private DosEventLog mergeFinalResult(DosEventLog eventLogByBaseline,DosEventLog eventLogByStaticThreshold){
        return eventLogByStaticThreshold;
    }

    private DosEventLog getDosEventLogByBaseline(DosSketchLog value, String destinationIp, String attackType) throws ParseException {
        Tuple2<ArrayList<Integer>, Integer> floodTypeTup = baselineMap.get(destinationIp).get(attackType);
        Integer base = getBaseValue(floodTypeTup, value);
        long diff = value.getSketch_sessions() - base;
        return getDosEventLog(value, base, diff);
    }

    private DosEventLog getDosEventLogByStaticThreshold(DosSketchLog value, Map<String, DosDetectionThreshold> thresholdMap) throws ParseException {
        DosEventLog result = null;
        String attackType = value.getAttack_type();
        if (thresholdMap.containsKey(attackType)) {
            DosDetectionThreshold threshold = thresholdMap.get(attackType);
            long base = threshold.getSessionsPerSec();
            long diff = value.getSketch_sessions() - base;
            result = getDosEventLog(value, base, diff);
        }
        return result;
    }

    private DosEventLog getDosEventLog(DosSketchLog value, long base, long diff) throws ParseException {
        DosEventLog result = null;
        String destinationIp = value.getDestination_ip();
        String attackType = value.getAttack_type();
        if (diff > 0 && base != 0) {
            String percent = getDiffPercent(diff, base);
            double diffPercentDouble = getDiffPercentDouble(percent);
            Severity severity = judgeSeverity(diffPercentDouble);
            if (severity != Severity.NORMAL) {
                result = getResult(value, severity, percent);
                logger.info("检测到当前server IP {} 存在 {} 异常,日志详情\n {}", destinationIp, attackType, result.toString());
            } else {
                logger.debug("当前server IP：{} 未出现 {} 异常,日志详情 {}", destinationIp, attackType, value.toString());
            }
        }
        return result;
    }

    private DosEventLog getResult(DosSketchLog value, Severity severity, String percent) {
        DosEventLog dosEventLog = new DosEventLog();
        dosEventLog.setLog_id(SnowflakeId.generateId());
        dosEventLog.setStart_time(value.getSketch_start_time());
        dosEventLog.setEnd_time(value.getSketch_start_time() + CommonConfig.FLINK_WINDOW_MAX_TIME);
        dosEventLog.setAttack_type(value.getAttack_type());
        dosEventLog.setSeverity(severity.toString());
        dosEventLog.setConditions(getConditions(percent));
        dosEventLog.setDestination_ip(value.getDestination_ip());
        dosEventLog.setDestination_country(IpUtils.ipLookup.countryLookup(value.getDestination_ip()));
        String ipList = value.getSource_ip();
        dosEventLog.setSource_ip_list(ipList);
        dosEventLog.setSource_country_list(getSourceCountryList(ipList));
        dosEventLog.setSession_rate(value.getSketch_sessions());
        dosEventLog.setPacket_rate(value.getSketch_packets());
        dosEventLog.setBit_rate(value.getSketch_bytes());
        return dosEventLog;
    }

    private Integer getBaseValue(Tuple2<ArrayList<Integer>, Integer> floodTypeTup, DosSketchLog value) {
        Integer base = 0;
        try {
            if (floodTypeTup != null) {
                ArrayList<Integer> baselines = floodTypeTup.f0;
                Integer defaultVaule = floodTypeTup.f1;
                if (baselines != null && baselines.size() == BASELINE_SIZE) {
                    int timeIndex = getCurrentTimeIndex(value.getSketch_start_time());
                    base = baselines.get(timeIndex);
                    if (base == 0) {
                        logger.debug("获取到当前IP: {},类型: {} baseline值为0,替换为P95观测值{}", value.getDestination_ip(), value.getAttack_type(), defaultVaule);
                        base = defaultVaule;
                    }
                }
            }
        } catch (Exception e) {
            logger.error("解析baseline数据失败,返回默认值0", e);
        }
        return base;
    }

    private String getConditions(String percent) {
        return "sessions > " + percent + " of baseline";
    }

    private String getSourceCountryList(String sourceIpList) {
        String[] ipArr = sourceIpList.split(",");
        HashSet<String> countrySet = new HashSet<>();
        for (String ip : ipArr) {
            countrySet.add(IpUtils.ipLookup.countryLookup(ip));
        }
        return StringUtils.join(countrySet, ",");
    }

    private int getCurrentTimeIndex(long sketchStartTime) {
        long currentDayTime = sketchStartTime / (60 * 60 * 24) * 60 * 60 * 24;
        long indexLong = (sketchStartTime - currentDayTime) / 600;
        return Integer.parseInt(Long.toString(indexLong));
    }

    private String getDiffPercent(long diff, long base) {
        double diffDou = Double.parseDouble(Long.toString(diff));
        double baseDou = Double.parseDouble(Long.toString(base));
        return PERCENT_INSTANCE.format(diffDou / baseDou);
    }

    private double getDiffPercentDouble(String diffPercent) throws ParseException {
        return PERCENT_INSTANCE.parse(diffPercent).doubleValue();
    }

    private Severity judgeSeverity(double diffPercent) {
        if (diffPercent >= CommonConfig.BASELINE_SESSIONS_MINOR_THRESHOLD && diffPercent < CommonConfig.BASELINE_SESSIONS_WARNING_THRESHOLD) {
            return Severity.MINOR;
        } else if (diffPercent >= CommonConfig.BASELINE_SESSIONS_WARNING_THRESHOLD && diffPercent < CommonConfig.BASELINE_SESSIONS_MAJOR_THRESHOLD) {
            return Severity.WARNING;
        } else if (diffPercent >= CommonConfig.BASELINE_SESSIONS_MAJOR_THRESHOLD && diffPercent < CommonConfig.BASELINE_SESSIONS_SEVERE_THRESHOLD) {
            return Severity.MAJOR;
        } else if (diffPercent >= CommonConfig.BASELINE_SESSIONS_SEVERE_THRESHOLD && diffPercent < CommonConfig.BASELINE_SESSIONS_CRITICAL_THRESHOLD) {
            return Severity.SEVERE;
        } else if (diffPercent >= CommonConfig.BASELINE_SESSIONS_CRITICAL_THRESHOLD) {
            return Severity.CRITICAL;
        } else {
            return Severity.NORMAL;
        }
    }

    private enum Severity {
        /**
         * 判断严重程度枚举类型
         */
        CRITICAL("Critical"),
        SEVERE("Severe"),
        MAJOR("Major"),
        WARNING("Warning"),
        MINOR("Minor"),
        NORMAL("Normal");

        private final String severity;

        @Override
        public String toString() {
            return this.severity;
        }

        Severity(String severity) {
            this.severity = severity;
        }

    }

}
