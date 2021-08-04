package com.zdjizhi.etl;

import com.zdjizhi.common.CommonConfig;
import com.zdjizhi.common.DosEventLog;
import com.zdjizhi.common.DosSketchLog;
import com.zdjizhi.sink.OutputStreamSink;
import com.zdjizhi.utils.IpUtils;
import com.zdjizhi.utils.SnowflakeId;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * @author wlh
 * DoS检测判断逻辑
 */
public class DosDetection extends BroadcastProcessFunction<DosSketchLog, Map<String, Map<String, List<Integer>>>, DosEventLog> {

    private static final Logger logger = LoggerFactory.getLogger(DosDetection.class);

    private final static int BASELINE_SIZE = 144;

    private static MapStateDescriptor<String, Map<String, Map<String, List<Integer>>>> descriptor = new MapStateDescriptor<>("boradcast-state",
            Types.STRING,
            new MapTypeInfo<>(String.class, new MapTypeInfo<>(String.class, (Class<List<Integer>>) (Class<?>) List.class).getTypeClass()));

    private final static NumberFormat PERCENT_INSTANCE = NumberFormat.getPercentInstance();

    @Override
    public void open(Configuration parameters) {
        PERCENT_INSTANCE.setMinimumFractionDigits(2);
    }

    @Override
    public void processElement(DosSketchLog value, ReadOnlyContext ctx, Collector<DosEventLog> out) throws Exception {
        try {
            Map<String, Map<String, List<Integer>>> broadcast = ctx.getBroadcastState(descriptor).get("broadcast-state");
            String destinationIp = value.getDestination_ip();
            String attackType = value.getAttack_type();
            logger.info("当前判断IP：{}, 类型: {}",destinationIp,attackType);
            if (broadcast.containsKey(destinationIp)){
                List<Integer> baseline = broadcast.get(destinationIp).get(attackType);
                if (baseline != null && baseline.size() == BASELINE_SIZE){
                    int timeIndex = getCurrentTimeIndex(value.getSketch_start_time());
                    Integer base = baseline.get(timeIndex);
                    long sketchSessions = value.getSketch_sessions();
                    long diff = sketchSessions - base;
                    if (diff > 0){
                        String percent = getDiffPercent(diff, sketchSessions);
                        double diffPercentDouble = getDiffPercentDouble(percent);
                        Severity severity = judgeSeverity(diffPercentDouble);
                        if (severity != Severity.NORMAL){
                            DosEventLog result = getResult(value, severity, percent);
                            logger.info("检测到当前server IP {} 存在 {} 异常,日志详情\n {}",destinationIp,attackType,result.toString());
                            out.collect(result);
                        }else {
                            logger.info("当前server IP：{} 未出现 {} 异常,日志详情 {}",destinationIp,attackType,value.toString());
                        }
                    }
                }
            }else {
                logger.info("未获取到当前server IP：{} 类型 {} baseline数据",destinationIp,attackType);
            }
        }catch (Exception e){
            logger.error("判定失败\n {} \n{}",value,e);
        }
    }

    @Override
    public void processBroadcastElement(Map<String, Map<String, List<Integer>>> value, Context ctx, Collector<DosEventLog> out) {
        try {
            ctx.getBroadcastState(descriptor).put("broadcast-state", value);
        }catch (Exception e){
            logger.error("更新广播状态失败 {}",e);
        }
    }

    public static void main(String[] args) {
        DosDetection dosDetection = new DosDetection();
//        HashSet<String> strings = new HashSet<>();
//        strings.add("13.46.241.36");
//        strings.add("25.46.241.45");
//        strings.add("133.46.241.53");
//        strings.add("219.46.242.74");
//        strings.add("153.146.241.196");
//        strings.add("132.46.241.21");
//        String join = StringUtils.join(strings, ",");
//        System.out.println(IpUtils.ipLookup.countryLookup("192.168.50.150"));
        System.out.println(Severity.CRITICAL.severity);
    }

    private DosEventLog getResult(DosSketchLog value,Severity severity,String percent){
        DosEventLog dosEventLog = new DosEventLog();
        dosEventLog.setLog_id(SnowflakeId.generateId());
        dosEventLog.setStart_time(value.getSketch_start_time());
        dosEventLog.setEnd_time(value.getSketch_start_time()+CommonConfig.FLINK_WINDOW_MAX_TIME);
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

    private String getConditions(String percent){
        return "sessions > "+percent+" of baseline";
    }

    private String getSourceCountryList(String sourceIpList){
        String[] ipArr = sourceIpList.split(",");
        HashSet<String> countrySet = new HashSet<>();
        for (String ip:ipArr){
            countrySet.add(IpUtils.ipLookup.countryLookup(ip));
        }
        return StringUtils.join(countrySet,",");
    }

    private int getCurrentTimeIndex(long sketchStartTime){
        long currentDayTime = sketchStartTime / (60 * 60 * 24) * 60 * 60 * 24;
        long indexLong = (sketchStartTime - currentDayTime) / 600;
        return Integer.parseInt(Long.toString(indexLong));
    }

    private String getDiffPercent(long diff,long sketchSessions){
        double diffDou = Double.parseDouble(Long.toString(diff));
        double sessDou = Double.parseDouble(Long.toString(sketchSessions));
        return PERCENT_INSTANCE.format(diffDou / sessDou);
    }

    private double getDiffPercentDouble(String diffPercent) throws ParseException {
        return PERCENT_INSTANCE.parse(diffPercent).doubleValue();
    }

    private Severity judgeSeverity(double diffPercent){
        if (diffPercent >= CommonConfig.BASELINE_SESSIONS_MINOR_THRESHOLD && diffPercent < CommonConfig.BASELINE_SESSIONS_WARNING_THRESHOLD){
            return Severity.MINOR;
        }else if (diffPercent >= CommonConfig.BASELINE_SESSIONS_WARNING_THRESHOLD && diffPercent < CommonConfig.BASELINE_SESSIONS_MAJOR_THRESHOLD){
            return Severity.WARNING;
        }else if (diffPercent >= CommonConfig.BASELINE_SESSIONS_MAJOR_THRESHOLD && diffPercent < CommonConfig.BASELINE_SESSIONS_SEVERE_THRESHOLD){
            return Severity.MAJOR;
        }else if (diffPercent >= CommonConfig.BASELINE_SESSIONS_SEVERE_THRESHOLD && diffPercent < CommonConfig.BASELINE_SESSIONS_CRITICAL_THRESHOLD){
            return Severity.SEVERE;
        }else if (diffPercent >= CommonConfig.BASELINE_SESSIONS_CRITICAL_THRESHOLD){
            return Severity.CRITICAL;
        }else {
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
