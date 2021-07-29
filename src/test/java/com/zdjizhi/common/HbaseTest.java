package com.zdjizhi.common;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;

public class HbaseTest {
    public static void main(String[] args) throws IOException {
        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();

        config.set("hbase.zookeeper.quorum", CommonConfig.HBASE_ZOOKEEPER_QUORUM);
        config.set("hbase.client.retries.number", "3");
        config.set("hbase.bulkload.retries.number", "3");
        config.set("zookeeper.recovery.retry", "3");
        config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, CommonConfig.HBASE_CLIENT_OPERATION_TIMEOUT);
        config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, CommonConfig.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD);

        TableName tableName  = TableName.valueOf("dos_test");
        Connection conn = ConnectionFactory.createConnection(config);
        Table table = conn.getTable(tableName);

        int[] arr = {1,3,4,5,1,2,4,4,15,6,37,234,1241};
        Put abcPut = new Put(Bytes.toBytes("abc"));
        abcPut.addColumn(Bytes.toBytes("attribute"),Bytes.toBytes("123"), WritableUtils.toByteArray(toWritable(arr)));
        table.put(abcPut);

        Get abcGet = new Get(Bytes.toBytes("abc"));
        Result r = table.get(abcGet);
        ArrayWritable w = new ArrayWritable(IntWritable.class);
        w.readFields(new DataInputStream(new ByteArrayInputStream(r.getValue(Bytes.toBytes("attribute"), Bytes.toBytes("123")))));
        ArrayList<Integer> arr2 = fromWritable(w);
        System.out.println(arr2.toString());

    }

    public static Writable toWritable(int[] arr) {
        Writable[] content = new Writable[arr.length];
        for (int i = 0; i < content.length; i++) {
            content[i] = new IntWritable(arr[i]);
        }
        return new ArrayWritable(IntWritable.class, content);
    }

    public static ArrayList<Integer> fromWritable(ArrayWritable writable) {
        Writable[] writables = ((ArrayWritable) writable).get();
        ArrayList<Integer> list = new ArrayList<Integer>(writables.length);
        for (Writable wrt : writables) {
            list.add(((IntWritable)wrt).get());
        }
        return list;
    }

}
