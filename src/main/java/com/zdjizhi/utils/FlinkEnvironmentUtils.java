package com.zdjizhi.utils;

import com.zdjizhi.common.CommonConfig;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @author wlh
 */
public class FlinkEnvironmentUtils {
    public static StreamExecutionEnvironment streamExeEnv = StreamExecutionEnvironment.getExecutionEnvironment();

    static {
        streamExeEnv.setParallelism(CommonConfig.STREAM_EXECUTION_ENVIRONMENT_PARALLELISM);

        /*
        // 每 1000ms 开始一次 checkpoint
        streamExeEnv.enableCheckpointing(CommonConfig.FLINK_WINDOW_MAX_TIME * 1000);

        // 设置模式为精确一次 (这是默认值)
        streamExeEnv.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 确认 checkpoints 之间的时间会进行 500 ms
        streamExeEnv.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        // Checkpoint 必须在一分钟内完成，否则就会被抛弃
        streamExeEnv.getCheckpointConfig().setCheckpointTimeout(60000);

        // 允许两个连续的 checkpoint 错误
        streamExeEnv.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);

        // 同一时间只允许一个 checkpoint 进行
        streamExeEnv.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // 使用 externalized checkpoints，这样 checkpoint 在作业取消后仍就会被保留
        streamExeEnv.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 开启实验性的 unaligned checkpoints
        streamExeEnv.getCheckpointConfig().enableUnalignedCheckpoints();
        */
    }

}
