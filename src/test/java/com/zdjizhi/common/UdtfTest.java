package com.zdjizhi.common;

import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

public class UdtfTest extends TableFunction<Row> {

    public void eval(Row[] rows) {
        for (Row row : rows) {
            collect(row);
        }
    }

    public static void main(String[] args) {

    }
}
