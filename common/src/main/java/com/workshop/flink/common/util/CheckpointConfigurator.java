package com.workshop.flink.common.util;

import com.workshop.flink.common.Constants;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public final class CheckpointConfigurator {

    public static void configure(StreamExecutionEnvironment env,
                                 CheckpointingMode mode,
                                 String storageBasePath) {
        env.enableCheckpointing(Constants.CHECKPOINT_INTERVAL_MS, mode);

        env.getCheckpointConfig().setCheckpointTimeout(Constants.CHECKPOINT_TIMEOUT_MS);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(
            Constants.MIN_PAUSE_BETWEEN_CHECKPOINTS_MS);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // Retain checkpoints on cancellation so participants can restore from them
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
            ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.getCheckpointConfig().setCheckpointStorage(
            new FileSystemCheckpointStorage(storageBasePath));
    }

    public static String defaultDir(String scenarioAndApp) {
        return System.getenv().getOrDefault(
            Constants.CHECKPOINT_DIR_ENV,
            "file:///tmp/flink-checkpoints/" + scenarioAndApp);
    }

    private CheckpointConfigurator() {}
}
