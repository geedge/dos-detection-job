package com.zdjizhi.common;

import com.zdjizhi.utils.CommonConfigurations;

/**
 * @author wlh
 * @date 2021/1/6
 */
public class CommonConfig {

    public static final int STREAM_EXECUTION_ENVIRONMENT_PARALLELISM = CommonConfigurations.getIntProperty("stream.execution.environment.parallelism");
    public static final String STREAM_EXECUTION_JOB_NAME = CommonConfigurations.getStringProperty("stream.execution.job.name");

    public static final int KAFKA_INPUT_PARALLELISM = CommonConfigurations.getIntProperty("kafka.input.parallelism");
    public static final String KAFKA_INPUT_TOPIC_NAME = CommonConfigurations.getStringProperty("kafka.input.topic.name");
    public static final String KAFKA_INPUT_BOOTSTRAP_SERVERS = CommonConfigurations.getStringProperty("kafka.input.bootstrap.servers");
    public static final String KAFKA_GROUP_ID = CommonConfigurations.getStringProperty("kafka.input.group.id");

    public static final int KAFKA_OUTPUT_METRIC_PARALLELISM = CommonConfigurations.getIntProperty("kafka.output.metric.parallelism");
    public static final String KAFKA_OUTPUT_METRIC_TOPIC_NAME = CommonConfigurations.getStringProperty("kafka.output.metric.topic.name");
    public static final int KAFKA_OUTPUT_EVENT_PARALLELISM = CommonConfigurations.getIntProperty("kafka.output.event.parallelism");
    public static final String KAFKA_OUTPUT_EVENT_TOPIC_NAME = CommonConfigurations.getStringProperty("kafka.output.event.topic.name");
    public static final String KAFKA_OUTPUT_BOOTSTRAP_SERVERS = CommonConfigurations.getStringProperty("kafka.output.bootstrap.servers");

    public static final String HBASE_ZOOKEEPER_QUORUM = CommonConfigurations.getStringProperty("hbase.zookeeper.quorum");
    public static final int HBASE_CLIENT_OPERATION_TIMEOUT = CommonConfigurations.getIntProperty("hbase.client.operation.timeout");
    public static final int HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD = CommonConfigurations.getIntProperty("hbase.client.scanner.timeout.period");

    public static final String HBASE_BASELINE_TABLE_NAME = CommonConfigurations.getStringProperty("hbase.baseline.table.name");
    public static final int HBASE_BASELINE_TOTAL_NUM = CommonConfigurations.getIntProperty("hbase.baseline.total.num");

    public static final int FLINK_FIRST_AGG_PARALLELISM = CommonConfigurations.getIntProperty("flink.first.agg.parallelism");
    public static final int FLINK_DETECTION_MAP_PARALLELISM = CommonConfigurations.getIntProperty("flink.detection.map.parallelism");
    public static final int FLINK_WATERMARK_MAX_ORDERNESS = CommonConfigurations.getIntProperty("flink.watermark.max.orderness");
    public static final int FLINK_WINDOW_MAX_TIME = CommonConfigurations.getIntProperty("flink.window.max.time");

    public static final int SOURCE_IP_LIST_LIMIT = CommonConfigurations.getIntProperty("source.ip.list.limit");
    public static final int DESTINATION_IP_PARTITION_NUM = CommonConfigurations.getIntProperty("destination.ip.partition.num");
    public static final int DATA_CENTER_ID_NUM = CommonConfigurations.getIntProperty("data.center.id.num");

    public static final String IP_MMDB_PATH = CommonConfigurations.getStringProperty("ip.mmdb.path");

    public static final int STATIC_SENSITIVITY_THRESHOLD = CommonConfigurations.getIntProperty("static.sensitivity.threshold");
    public static final double BASELINE_SENSITIVITY_THRESHOLD = CommonConfigurations.getDoubleProperty("baseline.sensitivity.threshold");

    public static final double BASELINE_SESSIONS_MINOR_THRESHOLD = CommonConfigurations.getDoubleProperty("baseline.sessions.minor.threshold");
    public static final double BASELINE_SESSIONS_WARNING_THRESHOLD = CommonConfigurations.getDoubleProperty("baseline.sessions.warning.threshold");
    public static final double BASELINE_SESSIONS_MAJOR_THRESHOLD = CommonConfigurations.getDoubleProperty("baseline.sessions.major.threshold");
    public static final double BASELINE_SESSIONS_SEVERE_THRESHOLD = CommonConfigurations.getDoubleProperty("baseline.sessions.severe.threshold");
    public static final double BASELINE_SESSIONS_CRITICAL_THRESHOLD = CommonConfigurations.getDoubleProperty("baseline.sessions.critical.threshold");

    public static final String BIFANG_SERVER_URI = CommonConfigurations.getStringProperty("bifang.server.uri");
    public static final String BIFANG_SERVER_TOKEN = CommonConfigurations.getStringProperty("bifang.server.token");
    public static final String BIFANG_SERVER_ENCRYPTPWD_PATH = CommonConfigurations.getStringProperty("bifang.server.encryptpwd.path");
    public static final String BIFANG_SERVER_LOGIN_PATH = CommonConfigurations.getStringProperty("bifang.server.login.path");
    public static final String BIFANG_SERVER_POLICY_THRESHOLD_PATH = CommonConfigurations.getStringProperty("bifang.server.policy.threshold.path");

    public static final int HTTP_POOL_MAX_CONNECTION = CommonConfigurations.getIntProperty("http.pool.max.connection");
    public static final int HTTP_POOL_MAX_PER_ROUTE = CommonConfigurations.getIntProperty("http.pool.max.per.route");
    public static final int HTTP_POOL_REQUEST_TIMEOUT = CommonConfigurations.getIntProperty("http.pool.request.timeout");
    public static final int HTTP_POOL_CONNECT_TIMEOUT = CommonConfigurations.getIntProperty("http.pool.connect.timeout");
    public static final int HTTP_POOL_RESPONSE_TIMEOUT = CommonConfigurations.getIntProperty("http.pool.response.timeout");

    public static final int STATIC_THRESHOLD_SCHEDULE_MINUTES = CommonConfigurations.getIntProperty("static.threshold.schedule.minutes");
    public static final int BASELINE_THRESHOLD_SCHEDULE_DAYS = CommonConfigurations.getIntProperty("baseline.threshold.schedule.days");

    public static final String SASL_JAAS_CONFIG_USER = CommonConfigurations.getStringProperty("sasl.jaas.config.user");
    public static final String SASL_JAAS_CONFIG_PASSWORD = CommonConfigurations.getStringProperty("sasl.jaas.config.password");

    public static final int SASL_JAAS_CONFIG_FLAG = CommonConfigurations.getIntProperty("sasl.jaas.config.flag");
}
