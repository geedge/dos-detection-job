package com.zdjizhi.utils;

import com.zdjizhi.common.CommonConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @author wlh
 */
public class FlinkEnvironmentUtils {
    public static StreamExecutionEnvironment streamExeEnv = StreamExecutionEnvironment.getExecutionEnvironment();

    static {
        streamExeEnv.setParallelism(CommonConfig.STREAM_EXECUTION_ENVIRONMENT_PARALLELISM);
    }

}
