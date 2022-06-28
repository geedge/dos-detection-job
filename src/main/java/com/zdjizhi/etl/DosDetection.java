package com.zdjizhi.etl;

import com.zdjizhi.common.*;
import com.zdjizhi.utils.*;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrBuilder;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.TreeRangeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.NumberFormat;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author wlh
 */
public class DosDetection extends RichMapFunction<DosSketchLog, DosEventLog> {

    private static final Logger logger = LoggerFactory.getLogger(DosDetection.class);
    private static Map<String, Map<String, DosBaselineThreshold>> baselineMap = new HashMap<>();
    private final static NumberFormat PERCENT_INSTANCE = NumberFormat.getPercentInstance();
    private HashMap<String, TreeRangeMap<IPAddress, DosDetectionThreshold>> thresholdRangeMap;

    private final static int BASELINE_SIZE = 144;
    private final static int STATIC_CONDITION_TYPE = 1;
    private final static int BASELINE_CONDITION_TYPE = 2;
    private final static int SENSITIVITY_CONDITION_TYPE = 3;

    private final static String SESSIONS_TAG = "sessions";
    private final static String PACKETS_TAG = "packets";
    private final static String BITS_TAG = "bits";

    private final static int OTHER_BASELINE_TYPE = 3;

    @Override
    public void open(Configuration parameters) {
        ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(2,
                new BasicThreadFactory.Builder().namingPattern("Dos-Detection-%d").daemon(true).build());
        try {
            executorService.scheduleAtFixedRate(() -> thresholdRangeMap = ParseStaticThreshold.createStaticThreshold(), 0,
                    CommonConfig.STATIC_THRESHOLD_SCHEDULE_MINUTES, TimeUnit.MINUTES);

            executorService.scheduleAtFixedRate(() -> baselineMap = ParseBaselineThreshold.readFromHbase(), 0,
                    CommonConfig.BASELINE_THRESHOLD_SCHEDULE_DAYS, TimeUnit.DAYS);
        } catch (Exception e) {
            logger.error("定时器任务执行失败", e);
        }
        PERCENT_INSTANCE.setMinimumFractionDigits(2);
    }

    @Override
    public DosEventLog map(DosSketchLog value) {
        DosEventLog finalResult = null;
        try {
            String destinationIp = value.getDestination_ip();
            String attackType = value.getAttack_type();
            IPAddress destinationIpAddress = new IPAddressString(destinationIp).getAddress();
            DosDetectionThreshold threshold = thresholdRangeMap.getOrDefault(attackType, TreeRangeMap.create()).get(destinationIpAddress);
            logger.debug("当前判断IP：{}, 类型: {}", destinationIp, attackType);
            if (threshold == null && baselineMap.containsKey(destinationIp)) {
                finalResult = getDosEventLogByBaseline(value);
            } else if (threshold == null && !baselineMap.containsKey(destinationIp)) {
                finalResult = getDosEventLogBySensitivityThreshold(value);
            } else if (threshold != null) {
                finalResult = getDosEventLogByStaticThreshold(value, threshold);
            } else {
                logger.debug("未获取到当前server IP：{} 类型 {} 静态阈值 和 baseline", destinationIp, attackType);
            }

        } catch (Exception e) {
            logger.error("判定失败\n {} \n{}", value, e);
        }
        return finalResult;
    }

    private DosEventLog getDosEventLogBySensitivityThreshold(DosSketchLog value) {
        DosEventLog result = null;
        long sketchSessions = value.getSketch_sessions();
        if (sketchSessions > NacosUtils.getIntProperty("static.sensitivity.threshold")) {
            long diff = sketchSessions - NacosUtils.getIntProperty("static.sensitivity.threshold");
            result = getDosEventLog(value, NacosUtils.getIntProperty("static.sensitivity.threshold"), diff, SENSITIVITY_CONDITION_TYPE, SESSIONS_TAG);
            result.setSeverity(Severity.MAJOR.severity);
        }
        return result;
    }

    private DosEventLog getDosEventLogByBaseline(DosSketchLog value) {
        DosEventLog result = null;
        String destinationIp = value.getDestination_ip();
        String attackType = value.getAttack_type();
        long sketchSessions = value.getSketch_sessions();
        if (sketchSessions > NacosUtils.getIntProperty("static.sensitivity.threshold")) {
            DosBaselineThreshold dosBaselineThreshold = baselineMap.get(destinationIp).get(attackType);
            Integer base = getBaseValue(dosBaselineThreshold, value);
            long diff = sketchSessions - base;
            result = getDosEventLog(value, base, diff, BASELINE_CONDITION_TYPE, SESSIONS_TAG);
        }
        return result;
    }

    private DosEventLog getDosEventLogByStaticThreshold(DosSketchLog value, DosDetectionThreshold threshold) {
        long base = threshold.getSessionsPerSec();
        long diff = value.getSketch_sessions() - base;
        DosEventLog result = getDosEventLog(value, base, diff, STATIC_CONDITION_TYPE, SESSIONS_TAG);
        if (result == null) {
            base = threshold.getPacketsPerSec();
            diff = value.getSketch_packets() - base;
            result = getDosEventLog(value, base, diff, STATIC_CONDITION_TYPE, PACKETS_TAG);
            if (result == null) {
                base = threshold.getBitsPerSec();
                diff = value.getSketch_bytes() - base;
                result = getDosEventLog(value, base, diff, STATIC_CONDITION_TYPE, BITS_TAG);
            }
        }
        return result;
    }

    private DosEventLog getDosEventLog(DosSketchLog value, long base, long diff, int type, String tag) {
        DosEventLog result = null;
        String destinationIp = value.getDestination_ip();
        String attackType = value.getAttack_type();
        if (diff > 0 && base != 0) {
            double percent = getDiffPercent(diff, base);
            Severity severity = judgeSeverity(percent);
            if (severity != Severity.NORMAL) {
                if (type == BASELINE_CONDITION_TYPE && percent < NacosUtils.getDoubleProperty("baseline.sensitivity.threshold")) {
                    logger.debug("当前server IP:{},类型:{},基线值{}百分比{}未超过基线敏感阈值,日志详情\n{}", destinationIp, attackType, base, percent, value);
                } else {
                    result = getResult(value, base, severity, percent+1, type, tag);
                    logger.info("检测到当前server IP {} 存在 {} 异常,超出基线{} {}倍,基于{}:{}检测,日志详情\n {}", destinationIp,attackType,base,percent,type,tag,result);
                }
            } else {
                logger.debug("当前server IP:{} 未出现 {} 异常,日志详情 {}", destinationIp, attackType, value);
            }
        }
        return result;
    }

    private DosEventLog getResult(DosSketchLog value, long base, Severity severity, double percent, int type, String tag) {
        DosEventLog dosEventLog = new DosEventLog();
        dosEventLog.setLog_id(SnowflakeId.generateId());
        dosEventLog.setStart_time(value.getSketch_start_time());
        dosEventLog.setEnd_time(value.getSketch_start_time() + value.getSketch_duration());
        dosEventLog.setAttack_type(value.getAttack_type());
        dosEventLog.setSeverity(severity.severity);
        dosEventLog.setConditions(getConditions(PERCENT_INSTANCE.format(percent), base, value.getSketch_sessions(), type, tag));
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

    private Integer getBaseValue(DosBaselineThreshold dosBaselineThreshold, DosSketchLog value) {
        Integer base = 0;
        try {
            if (dosBaselineThreshold != null) {
                ArrayList<Integer> baselines = dosBaselineThreshold.getSession_rate();
                Integer defaultVaule = dosBaselineThreshold.getSession_rate_default_value();
                Integer sessionRateBaselineType = dosBaselineThreshold.getSession_rate_baseline_type();
                if (baselines != null && baselines.size() == BASELINE_SIZE) {
                    int timeIndex = getCurrentTimeIndex(value.getSketch_start_time());
                    base = baselines.get(timeIndex);
                    if (base == 0) {
                        logger.debug("获取到当前IP: {},类型: {} baseline值为0,替换为P95观测值{}", value.getDestination_ip(), value.getAttack_type(), defaultVaule);
                        base = defaultVaule;
                    }
                    if (sessionRateBaselineType == OTHER_BASELINE_TYPE && base < NacosUtils.getIntProperty("static.sensitivity.threshold")){
                        base = NacosUtils.getIntProperty("static.sensitivity.threshold");
                    }
                }
            }
        } catch (Exception e) {
            logger.error("解析baseline数据失败,返回默认值0", e);
        }
        return base;
    }

    private String getConditions(String percent, long base, long sessions, int type, String tag) {
        switch (type) {
            case STATIC_CONDITION_TYPE:
                return new StrBuilder()
                        .append("Rate > ")
                        .append(base).append(" ")
                        .append(tag).append("/s")
                        .toString();
            case BASELINE_CONDITION_TYPE:
                return new StrBuilder()
                        .append(tag).append(" > ")
                        .append(percent).append(" of baseline")
                        .toString();
            case SENSITIVITY_CONDITION_TYPE:
                return new StrBuilder()
                        .append(sessions).append(" ")
                        .append(tag).append("/s Unusually high ")
                        .append(StringUtils.capitalize(tag))
                        .toString();
            default:
                throw new IllegalArgumentException("Illegal Argument type:" + type + ", known types = [1,2,3]");
        }
    }

    private String getSourceCountryList(String sourceIpList) {
        if (StringUtil.isNotBlank(sourceIpList)) {
            String countryList;
            try {
                String[] ipArr = sourceIpList.split(",");
                HashSet<String> countrySet = new HashSet<>();
                for (String ip : ipArr) {
                    String country = IpUtils.ipLookup.countryLookup(ip);
                    if (StringUtil.isNotBlank(country)){
                        countrySet.add(country);
                    }
                }
                countryList = StringUtils.join(countrySet, ", ");
                return countryList;
            } catch (Exception e) {
                logger.error("{} source IP lists 获取国家失败", sourceIpList, e);
                return StringUtil.EMPTY;
            }
        } else {
            throw new IllegalArgumentException("Illegal Argument sourceIpList = null");
        }
    }

    private int getCurrentTimeIndex(long sketchStartTime) {
        int index = 0;
        try {
            long currentDayTime = DateUtils.getTimeFloor(new Date(sketchStartTime * 1000L), "P1D").getTime() / 1000;
            long indexLong = (sketchStartTime - currentDayTime) / (86400 / BASELINE_SIZE);
            index = Integer.parseInt(Long.toString(indexLong));
        } catch (Exception e) {
            logger.error("获取time index失败", e);
        }
        return index;
    }

    public static void main(String[] args) {
//        System.out.println(new DosDetection().getSourceCountryList("192.0.2.3,138.199.14.31,255.255.255.255,121.14.89.209," +
//                "23.200.74.224,161.117.68.253"));
//        DosDetection dosDetection = new DosDetection();
//        System.out.println(dosDetection.judgeSeverity(dosDetection.getDiffPercent(499, 1000)));

    }

    private Double getDiffPercent(long diff, long base) {
        return BigDecimal.valueOf((float) diff / base).setScale(4, BigDecimal.ROUND_HALF_UP).doubleValue();
    }

    private Severity judgeSeverity(double diffPercent) {
        if (diffPercent >= NacosUtils.getDoubleProperty("baseline.sessions.minor.threshold") && diffPercent < NacosUtils.getDoubleProperty("baseline.sessions.warning.threshold")) {
            return Severity.MINOR;
        } else if (diffPercent >= NacosUtils.getDoubleProperty("baseline.sessions.warning.threshold") && diffPercent < NacosUtils.getDoubleProperty("baseline.sessions.major.threshold")) {
            return Severity.WARNING;
        } else if (diffPercent >= NacosUtils.getDoubleProperty("baseline.sessions.major.threshold") && diffPercent < NacosUtils.getDoubleProperty("baseline.sessions.severe.threshold")) {
            return Severity.MAJOR;
        } else if (diffPercent >= NacosUtils.getDoubleProperty("baseline.sessions.severe.threshold") && diffPercent < NacosUtils.getDoubleProperty("baseline.sessions.critical.threshold")) {
            return Severity.SEVERE;
        } else if (diffPercent >= NacosUtils.getDoubleProperty("baseline.sessions.critical.threshold")) {
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
