package com.zdjizhi.utils;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.*;

/**
 * @author wlh
 */
public class HbaseUtils {

    public static Integer getIntegerValue(Result result, String family, String qualifier) {
        byte[] value = result.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        if (value != null){
            return Bytes.toInt(value);
        }
        return 1;
    }

    public static ArrayList<Integer> getArraylist(Result result, String family, String qualifier) throws IOException {
        if (containsColumn(result, family, qualifier)) {
            ArrayWritable w = new ArrayWritable(IntWritable.class);
            w.readFields(new DataInputStream(new ByteArrayInputStream(result.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier)))));
            return fromWritable(w);
        }
        return null;
    }

    private static ArrayList<Integer> fromWritable(ArrayWritable writable) {
        Writable[] writables = writable.get();
        ArrayList<Integer> list = new ArrayList<>(writables.length);
        for (Writable wrt : writables) {
            list.add(((IntWritable) wrt).get());
        }
        return list;
    }

    private static boolean containsColumn(Result result, String family, String qualifier) {
        return result.containsColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
    }

}
