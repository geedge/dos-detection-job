package com.zdjizhi.source;

import com.zdjizhi.common.CommonConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author wlh
 */
public class BaselineSource extends RichSourceFunction<Map<String, Map<String,List<Integer>>>> {

    private static final Logger logger = LoggerFactory.getLogger(BaselineSource.class);
    private Connection conn  = null;
    private Table table  = null;
    private Scan scan = null;

    @Override
    public void open(Configuration parameters) throws Exception {

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

//                .addFamily(Bytes.toBytes(CommonConfig.HBASE_BASELINE_FAMLIY_NAME));
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void run(SourceContext<Map<String, Map<String,List<Integer>>>> sourceContext) throws Exception {
        logger.info("开始读取baseline数据");
        ResultScanner rs = table.getScanner(scan);
//        Map<String, List<Integer>[]> baselineMap = new HashMap<>();
        Map<String, Map<String,List<Integer>>> baselineMap = new HashMap<>();
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
//            List[] arr = new ArrayList[]{tcp,udp,icmp,dns};
            baselineMap.put(rowkey,floodTypeMap);
        }
        sourceContext.collect(baselineMap);
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

    @Override
    public void cancel() {
        try {
            if (table != null) {
                table.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        DataStreamSource<Map<String, Map<String,List<Integer>>>> mapDataStreamSource = env.addSource(new BaselineSource());
        DataStream<Map<String, Map<String,List<Integer>>>> broadcast = mapDataStreamSource.broadcast();
        mapDataStreamSource.print();
        env.execute();
    }
}
