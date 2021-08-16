package com.zdjizhi.utils;

import com.zdjizhi.common.CommonConfig;
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
import java.util.*;

/**
 * @author wlh
 */
public class HbaseUtils {
    private static final Logger logger = LoggerFactory.getLogger(HbaseUtils.class);
    private static Table table  = null;
    private static Scan scan = null;
    public static Map<String, Map<String,List<Integer>>> baselineMap = new HashMap<>();

    static {
        readFromHbase();
    }

    private static void prepareHbaseEnv() throws IOException {
        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();

        config.set("hbase.zookeeper.quorum", CommonConfig.HBASE_ZOOKEEPER_QUORUM);
        config.set("hbase.client.retries.number", "3");
        config.set("hbase.bulkload.retries.number", "3");
        config.set("zookeeper.recovery.retry", "3");
        config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, CommonConfig.HBASE_CLIENT_OPERATION_TIMEOUT);
        config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, CommonConfig.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD);

        TableName tableName  = TableName.valueOf(CommonConfig.HBASE_BASELINE_TABLE_NAME);
        Connection conn = ConnectionFactory.createConnection(config);
        table = conn.getTable(tableName);
        scan = new Scan().setAllowPartialResults(true).setLimit(CommonConfig.HBASE_BASELINE_TOTAL_NUM);
        logger.info("连接hbase成功，正在读取baseline数据");
    }

    public static void main(String[] args) {
        Set<String> keySet = baselineMap.keySet();
        for (String key:keySet){
            Map<String, List<Integer>> stringListMap = baselineMap.get(key);
            Set<String> typeSet = stringListMap.keySet();
            for (String type:typeSet){
                List<Integer> lines = stringListMap.get(type);
                if (lines != null){
                    System.out.println(key+"--"+type+"--"+Arrays.toString(lines.toArray()));
                }
            }
        }
        System.out.println(baselineMap.size());
    }

    private static void readFromHbase(){
        try {
            prepareHbaseEnv();
            logger.info("开始读取baseline数据");
            ResultScanner rs = table.getScanner(scan);
            for (Result result : rs) {
                Map<String, List<Integer>> floodTypeMap = new HashMap<>();
                String rowkey = Bytes.toString(result.getRow());
                ArrayList<Integer> tcp = getArraylist(result,"TCP SYN Flood", "session_rate");
                ArrayList<Integer> udp = getArraylist(result,"UDP Flood", "session_rate");
                ArrayList<Integer> icmp = getArraylist(result,"ICMP Flood", "session_rate");
                ArrayList<Integer> dns = getArraylist(result,"DNS Amplification", "session_rate");
                floodTypeMap.put("TCP SYN Flood",tcp);
                floodTypeMap.put("UDP Flood",udp);
                floodTypeMap.put("ICMP Flood",icmp);
                floodTypeMap.put("DNS Amplification",dns);
                baselineMap.put(rowkey,floodTypeMap);
            }
            logger.info("格式化baseline数据成功，读取IP共：{}",baselineMap.size());
        }catch (Exception e){
            logger.error("读取hbase数据失败",e);
        }
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


}
