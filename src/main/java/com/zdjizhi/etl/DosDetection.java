package com.zdjizhi.etl;

import com.zdjizhi.common.CommonConfig;
import com.zdjizhi.common.DosDetectionThreshold;
import com.zdjizhi.common.DosEventLog;
import com.zdjizhi.common.DosSketchLog;
import com.zdjizhi.utils.*;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrBuilder;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
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
    private static Map<String, Map<String, Tuple2<ArrayList<Integer>, Integer>>> baselineMap = new HashMap<>();
    private final static int BASELINE_SIZE = 144;
    private final static NumberFormat PERCENT_INSTANCE = NumberFormat.getPercentInstance();
    private TreeRangeMap<IPAddress, Map<String, DosDetectionThreshold>> thresholdRangeMap;

    @Override
    public void open(Configuration parameters) {
        ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(2,
                new BasicThreadFactory.Builder().namingPattern("Dos-Detection-%d").daemon(true).build());
        try {
            executorService.scheduleAtFixedRate(() -> thresholdRangeMap = ParseStaticThreshold.createStaticThreshold(), 0,
                    CommonConfig.STATIC_THRESHOLD_SCHEDULE_MINUTES, TimeUnit.MINUTES);

            executorService.scheduleAtFixedRate(() -> baselineMap = HbaseUtils.readFromHbase(), 0,
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
            Map<String, DosDetectionThreshold> thresholdMap = thresholdRangeMap.get(destinationIpAddress);
            logger.debug("当前判断IP：{}, 类型: {}", destinationIp, attackType);
            if ((thresholdMap == null || !thresholdMap.containsKey(attackType)) && baselineMap.containsKey(destinationIp)) {
                finalResult = getDosEventLogByBaseline(value);
            } else if ((thresholdMap == null || !thresholdMap.containsKey(attackType)) && !baselineMap.containsKey(destinationIp)) {
                finalResult = getDosEventLogBySensitivityThreshold(value);
            } else if (thresholdMap != null && thresholdMap.containsKey(attackType)) {
                finalResult = getDosEventLogByStaticThreshold(value, thresholdMap);
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
        if (sketchSessions > CommonConfig.STATIC_SENSITIVITY_THRESHOLD) {
            result = getDosEventLog(value, CommonConfig.STATIC_SENSITIVITY_THRESHOLD, sketchSessions - CommonConfig.STATIC_SENSITIVITY_THRESHOLD, 3, "sessions");
            result.setSeverity(Severity.MAJOR.severity);
        }
        return result;
    }

    private DosEventLog getDosEventLogByBaseline(DosSketchLog value) {
        DosEventLog result = null;
        String destinationIp = value.getDestination_ip();
        String attackType = value.getAttack_type();
        long sketchSessions = value.getSketch_sessions();
        if (sketchSessions > CommonConfig.STATIC_SENSITIVITY_THRESHOLD) {
            Tuple2<ArrayList<Integer>, Integer> floodTypeTup = baselineMap.get(destinationIp).get(attackType);
            Integer base = getBaseValue(floodTypeTup, value);
            result = getDosEventLog(value, base, sketchSessions - base, 2, "sessions");
        }
        return result;
    }

    private DosEventLog getDosEventLogByStaticThreshold(DosSketchLog value, Map<String, DosDetectionThreshold> thresholdMap) {
        DosEventLog result = null;
        String attackType = value.getAttack_type();
        long base, diff;
        if (thresholdMap.containsKey(attackType)) {
            DosDetectionThreshold threshold = thresholdMap.get(attackType);
            base = threshold.getSessionsPerSec();
            diff = value.getSketch_sessions() - base;
            result = getDosEventLog(value, base, diff, 1, "sessions");
            if (result == null) {
                base = threshold.getPacketsPerSec();
                diff = value.getSketch_packets() - base;
                result = getDosEventLog(value, base, diff, 1, "packets");
                if (result == null) {
                    base = threshold.getBitsPerSec();
                    diff = value.getSketch_bytes() - base;
                    result = getDosEventLog(value, base, diff, 1, "bits");
                }
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
                if (type == 2 && percent < CommonConfig.BASELINE_SENSITIVITY_THRESHOLD) {
                    logger.debug("当前server IP:{},类型:{},基线值{}百分比{}未超过基线敏感阈值,日志详情\n{}", destinationIp, attackType, base, percent, value);
                } else {
                    result = getResult(value, base, severity, percent, type, tag);
                    logger.info("检测到当前server IP {} 存在 {} 异常,超出基线{} {}倍,基于{}检测,日志详情\n {}", destinationIp, attackType, base, percent, type, result);
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

    private String getConditions(String percent, long base, long sessions, int type, String tag) {
        switch (type) {
            case 1:
                return new StrBuilder()
                        .append(tag).append(" > ")
                        .append(base).append(" ")
                        .append(tag).append("/s")
                        .toString();
            case 2:
                return new StrBuilder()
                        .append(tag).append(" > ")
                        .append(percent).append(" of baseline")
                        .toString();
            case 3:
                return new StrBuilder()
                        .append(sessions).append(" ")
                        .append(tag).append("/s Unusually high ")
                        .append(StringUtils.capitalize(tag))
                        .toString();
            default:
                throw new IllegalArgumentException("Illegal Argument " + type + ", known types = [1,2,3]");
        }
    }

    private String getSourceCountryList(String sourceIpList) {
        if (StringUtil.isNotBlank(sourceIpList)) {
            String countryList;
            try {
                String[] ipArr = sourceIpList.split(",");
                HashSet<String> countrySet = new HashSet<>();
                for (String ip : ipArr) {
                    countrySet.add(IpUtils.ipLookup.countryLookup(ip));
                }
                countryList = StringUtils.join(countrySet, ",");
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
        Date date = new Date(1631548860 * 1000L);
        System.out.println(date);
        Date p1D = DateUtils.getTimeFloor(date, "P1D");
        System.out.println(p1D + " " + p1D.getTime() / 1000);
        System.out.println(new DosDetection().getCurrentTimeIndex(1634659080));
        System.out.println(new DosDetection().getConditions(PERCENT_INSTANCE.format(1.64862), 100, 100, 3, "packets"));
        System.out.println(10 + 10 * 0.2);
    }

    private Double getDiffPercent(long diff, long base) {
        return BigDecimal.valueOf((float) diff / base).setScale(4, BigDecimal.ROUND_HALF_UP).doubleValue();
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
        CRITICAL("Critical", 5),
        SEVERE("Severe", 4),
        MAJOR("Major", 3),
        WARNING("Warning", 2),
        MINOR("Minor", 1),
        NORMAL("Normal", 0);

        private final String severity;
        private final int score;

        @Override
        public String toString() {
            return this.severity;
        }

        Severity(String severity, int score) {
            this.severity = severity;
            this.score = score;
        }

    }

    @Deprecated
    private DosEventLog mergeFinalResult(Tuple2<Severity, DosEventLog> eventLogByBaseline, Tuple2<Severity, DosEventLog> eventLogByStaticThreshold) {
        if (eventLogByBaseline.f0.score > eventLogByStaticThreshold.f0.score) {
            logger.info("merge eventLogByBaseline {} \neventLogByStaticThreshold {}", eventLogByBaseline, eventLogByStaticThreshold);
            return mergeCondition(eventLogByBaseline.f1, eventLogByStaticThreshold.f1);
        } else {
            logger.info("merge eventLogByStaticThreshold {} \neventLogByBaseline {}", eventLogByStaticThreshold, eventLogByBaseline);
            return mergeCondition(eventLogByStaticThreshold.f1, eventLogByBaseline.f1);
        }
    }

    @Deprecated
    private DosEventLog mergeCondition(DosEventLog log1, DosEventLog log2) {
        if (log1 != null && log2 != null) {
            String conditions1 = log1.getConditions();
            String conditions2 = log2.getConditions();
            log1.setConditions(conditions1 + " and " + conditions2);
        } else if (log1 == null && log2 != null) {
            log1 = log2;
        }
        return log1;
    }

}
