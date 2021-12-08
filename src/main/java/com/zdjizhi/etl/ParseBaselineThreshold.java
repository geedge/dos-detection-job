package com.zdjizhi.etl;

import com.zdjizhi.common.CommonConfig;
import com.zdjizhi.common.DosBaselineThreshold;
import com.zdjizhi.utils.DateUtils;
import com.zdjizhi.utils.HbaseUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class ParseBaselineThreshold {

    private static final Logger logger = LoggerFactory.getLogger(ParseBaselineThreshold.class);
    private static ArrayList<String> floodTypeList = new ArrayList<>();

    private static Table table = null;
    private static Scan scan = null;

    static {
        floodTypeList.add("TCP SYN Flood");
        floodTypeList.add("UDP Flood");
        floodTypeList.add("ICMP Flood");
        floodTypeList.add("DNS Flood");
    }

    private static void prepareHbaseEnv() throws IOException {
        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();

        config.set("hbase.zookeeper.quorum", CommonConfig.HBASE_ZOOKEEPER_QUORUM);
        config.set("hbase.client.retries.number", "3");
        config.set("hbase.bulkload.retries.number", "3");
        config.set("zookeeper.recovery.retry", "3");
        config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, CommonConfig.HBASE_CLIENT_OPERATION_TIMEOUT);
        config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, CommonConfig.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD);

        TableName tableName = TableName.valueOf(CommonConfig.HBASE_BASELINE_TABLE_NAME);
        Connection conn = ConnectionFactory.createConnection(config);
        table = conn.getTable(tableName);
        long currentTimeMillis = System.currentTimeMillis();
        scan = new Scan()
                .setAllowPartialResults(true)
                .setTimeRange(DateUtils.getSomeDate(new Date(currentTimeMillis), Math.negateExact(CommonConfig.HBASE_BASELINE_TTL)).getTime(), currentTimeMillis)
                .setLimit(CommonConfig.HBASE_BASELINE_TOTAL_NUM);
        logger.info("连接hbase成功，正在读取baseline数据");
    }


    static Map<String, Map<String, DosBaselineThreshold>> readFromHbase() {
        Map<String, Map<String, DosBaselineThreshold>> baselineMap = new HashMap<>();
        try {
            prepareHbaseEnv();
            logger.info("开始读取baseline数据");
            ResultScanner rs = table.getScanner(scan);
            for (Result result : rs) {
                Map<String, DosBaselineThreshold> floodTypeMap = new HashMap<>();
                String rowkey = Bytes.toString(result.getRow());
                for (String type:floodTypeList){
                    DosBaselineThreshold baselineThreshold = new DosBaselineThreshold();
                    ArrayList<Integer> sessionRate = HbaseUtils.getArraylist(result, type, "session_rate");
                    if (sessionRate != null && !sessionRate.isEmpty()){
                        Integer defaultValue = HbaseUtils.getIntegerValue(result, type, "session_rate_default_value");
                        Integer rateBaselineType = HbaseUtils.getIntegerValue(result, type, "session_rate_baseline_type");
                        baselineThreshold.setSession_rate(sessionRate);
                        baselineThreshold.setSession_rate_default_value(defaultValue);
                        baselineThreshold.setSession_rate_baseline_type(rateBaselineType);
                        floodTypeMap.put(type,baselineThreshold);
                    }
                }
                baselineMap.put(rowkey, floodTypeMap);
            }
            logger.info("格式化baseline数据成功，读取IP共：{}", baselineMap.size());
        } catch (Exception e) {
            logger.error("读取hbase数据失败", e);
        }
        return baselineMap;
    }

    public static void main(String[] args) {
        long currentTimeMillis = System.currentTimeMillis();
        long p200D = DateUtils.getSomeDate(new Date(currentTimeMillis), Math.negateExact(CommonConfig.HBASE_BASELINE_TTL)).getTime();
        System.out.println(p200D);
        System.out.println(currentTimeMillis);
        System.out.println(currentTimeMillis - p200D);


        Map<String, Map<String, DosBaselineThreshold>> baselineMap = readFromHbase();
        Set<String> keySet = baselineMap.keySet();
        for (String key : keySet) {
            Map<String, DosBaselineThreshold> stringTuple2Map = baselineMap.get(key);
            Set<String> strings = stringTuple2Map.keySet();
            for (String s:strings){
                DosBaselineThreshold dosBaselineThreshold = stringTuple2Map.get(s);
                System.out.println(key+"---"+s+"---"+dosBaselineThreshold);
            }
        }
        System.out.println(baselineMap.size());
    }




}
