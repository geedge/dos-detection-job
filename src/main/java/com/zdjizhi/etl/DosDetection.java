package com.zdjizhi.etl;

import com.zdjizhi.common.CommonConfig;
import com.zdjizhi.common.DosEventLog;
import com.zdjizhi.common.DosSketchLog;
import com.zdjizhi.utils.IpUtils;
import com.zdjizhi.utils.SnowflakeId;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.*;

public class DosDetection extends RichMapFunction<DosSketchLog, DosEventLog> {

    private static final Logger logger = LoggerFactory.getLogger(DosDetection.class);
    private Connection conn  = null;
    private Table table  = null;
    private Scan scan = null;
    private Map<String, Map<String,List<Integer>>> baselineMap = new HashMap<>();
    private final static int BASELINE_SIZE = 144;
    private final static NumberFormat PERCENT_INSTANCE = NumberFormat.getPercentInstance();

    @Override
    public void open(Configuration parameters) throws Exception {
        readFromHbase();
        PERCENT_INSTANCE.setMinimumFractionDigits(2);
    }

    @Override
    public DosEventLog map(DosSketchLog value) throws Exception {
        try {
            String destinationIp = value.getDestination_ip();
            String attackType = value.getAttack_type();
            logger.debug("当前判断IP：{}, 类型: {}",destinationIp,attackType);
            if (baselineMap.containsKey(destinationIp)){
                List<Integer> baseline = baselineMap.get(destinationIp).get(attackType);
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
                            return result;
                        }else {
                            logger.debug("当前server IP：{} 未出现 {} 异常,日志详情 {}",destinationIp,attackType,value.toString());
                        }
                    }
                }
            }else {
                logger.debug("未获取到当前server IP：{} 类型 {} baseline数据",destinationIp,attackType);
            }
        }catch (Exception e){
            logger.error("判定失败\n {} \n{}",value,e);
        }
        return null;
    }

    private void prepareHbaseEnv() throws IOException {
        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();

        config.set("hbase.zookeeper.quorum", CommonConfig.HBASE_ZOOKEEPER_QUORUM);
        config.set("hbase.client.retries.number", "3");
        config.set("hbase.bulkload.retries.number", "3");
        config.set("zookeeper.recovery.retry", "3");
        config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, CommonConfig.HBASE_CLIENT_OPERATION_TIMEOUT);
        config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, CommonConfig.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD);

        TableName tableName  = TableName.valueOf(CommonConfig.HBASE_BASELINE_TABLE_NAME);
        conn = ConnectionFactory.createConnection(config);
        table = conn.getTable(tableName);
        scan = new Scan().setAllowPartialResults(true).setLimit(CommonConfig.HBASE_BASELINE_TOTAL_NUM);
        logger.info("连接hbase成功，正在读取baseline数据");
    }

    private void readFromHbase() throws IOException {
        prepareHbaseEnv();
        logger.info("开始读取baseline数据");
        ResultScanner rs = table.getScanner(scan);
        for (Result result : rs) {
            Map<String, List<Integer>> floodTypeMap = new HashMap<>();
            String rowkey = Bytes.toString(result.getRow());
            ArrayList<Integer> tcp = getArraylist(result,"TCP SYN Flood", "session_num");
            ArrayList<Integer> udp = getArraylist(result,"UDP Flood", "session_num");
            ArrayList<Integer> icmp = getArraylist(result,"ICMP Flood", "session_num");
            ArrayList<Integer> dns = getArraylist(result,"DNS Amplification", "session_num");
            floodTypeMap.put("TCP SYN Flood",tcp);
            floodTypeMap.put("UDP Flood",udp);
            floodTypeMap.put("ICMP Flood",icmp);
            floodTypeMap.put("DNS Amplification",dns);
            baselineMap.put(rowkey,floodTypeMap);
        }
        logger.info("格式化baseline数据成功，读取IP共：{}",baselineMap.size());
    }

    private static ArrayList<Integer> getArraylist(Result result,String family,String qualifier) throws IOException {
        if (!result.containsColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier))){
            return null;
        }
        ArrayWritable w = new ArrayWritable(IntWritable.class);
        w.readFields(new DataInputStream(new ByteArrayInputStream(result.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier)))));
        return fromWritable(w);
    }

    private static ArrayList<Integer> fromWritable(ArrayWritable writable) {
        Writable[] writables = writable.get();
        ArrayList<Integer> list = new ArrayList<>(writables.length);
        for (Writable wrt : writables) {
            list.add(((IntWritable)wrt).get());
        }
        return list;
    }

    private DosEventLog getResult(DosSketchLog value, Severity severity, String percent){
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
