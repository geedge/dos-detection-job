package com.zdjizhi.common;

import com.zdjizhi.utils.CommonConfigurations;

/**
 * Created by wk on 2021/1/6.
 */
public class CommonConfig {

    public static final int STREAM_EXECUTION_ENVIRONMENT_PARALLELISM = CommonConfigurations.getIntProperty("stream.execution.environment.parallelism");
    public static final String STREAM_EXECUTION_JOB_NAME = CommonConfigurations.getStringProperty("stream.execution.job.name");

    public static final int KAFKA_INPUT_PARALLELISM = CommonConfigurations.getIntProperty("kafka.input.parallelism");
    public static final String KAFKA_INPUT_TOPIC_NAME = CommonConfigurations.getStringProperty("kafka.input.topic.name");
    public static final String KAFKA_INPUT_BOOTSTRAP_SERVERS = CommonConfigurations.getStringProperty("kafka.input.bootstrap.servers");
    public static final String KAFKA_SCAN_STARTUP_MODE = CommonConfigurations.getStringProperty("kafka.input.scan.startup.mode");
    public static final String KAFKA_GROUP_ID = CommonConfigurations.getStringProperty("kafka.input.group.id");

    public static final int KAFKA_OUTPUT_METRIC_PARALLELISM = CommonConfigurations.getIntProperty("kafka.output.metric.parallelism");
    public static final String KAFKA_OUTPUT_METRIC_TOPIC_NAME = CommonConfigurations.getStringProperty("kafka.output.metric.topic.name");
    public static final int KAFKA_OUTPUT_EVENT_PARALLELISM = CommonConfigurations.getIntProperty("kafka.output.event.parallelism");
    public static final String KAFKA_OUTPUT_EVENT_TOPIC_NAME = CommonConfigurations.getStringProperty("kafka.output.event.topic.name");
    public static final String KAFKA_OUTPUT_BOOTSTRAP_SERVERS = CommonConfigurations.getStringProperty("kafka.output.bootstrap.servers");

    public static final int HBASE_INPUT_PARALLELISM = CommonConfigurations.getIntProperty("hbase.input.parallelism");
    public static final String HBASE_ZOOKEEPER_QUORUM = CommonConfigurations.getStringProperty("hbase.zookeeper.quorum");
    public static final int HBASE_CLIENT_OPERATION_TIMEOUT = CommonConfigurations.getIntProperty("hbase.client.operation.timeout");
    public static final int HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD = CommonConfigurations.getIntProperty("hbase.client.scanner.timeout.period");

    public static final String HBASE_BASELINE_TABLE_NAME = CommonConfigurations.getStringProperty("hbase.baseline.table.name");
    public static final int HBASE_BASELINE_TOTAL_NUM = CommonConfigurations.getIntProperty("hbase.baseline.total.num");

    public static final int FLINK_FIRST_AGG_PARALLELISM = CommonConfigurations.getIntProperty("flink.first.agg.parallelism");
    public static final int FLINK_SECOND_AGG_PARALLELISM = CommonConfigurations.getIntProperty("flink.second.agg.parallelism");
    public static final int FLINK_WATERMARK_MAX_ORDERNESS = CommonConfigurations.getIntProperty("flink.watermark.max.orderness");
    public static final int FLINK_WINDOW_MAX_TIME = CommonConfigurations.getIntProperty("flink.window.max.time");

    public static final int SOURCE_IP_LIST_LIMIT = CommonConfigurations.getIntProperty("source.ip.list.limit");
    public static final int DATA_CENTER_ID_NUM = CommonConfigurations.getIntProperty("data.center.id.num");

    public static final String IP_MMDB_PATH = CommonConfigurations.getStringProperty("ip.mmdb.path");

    public static final double BASELINE_SESSIONS_MINOR_THRESHOLD = CommonConfigurations.getDoubleProperty("baseline.sessions.minor.threshold");
    public static final double BASELINE_SESSIONS_WARNING_THRESHOLD = CommonConfigurations.getDoubleProperty("baseline.sessions.warning.threshold");
    public static final double BASELINE_SESSIONS_MAJOR_THRESHOLD = CommonConfigurations.getDoubleProperty("baseline.sessions.major.threshold");
    public static final double BASELINE_SESSIONS_SEVERE_THRESHOLD = CommonConfigurations.getDoubleProperty("baseline.sessions.severe.threshold");
    public static final double BASELINE_SESSIONS_CRITICAL_THRESHOLD = CommonConfigurations.getDoubleProperty("baseline.sessions.critical.threshold");

}
