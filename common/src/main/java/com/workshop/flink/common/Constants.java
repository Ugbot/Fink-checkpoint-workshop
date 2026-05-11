package com.workshop.flink.common;

public final class Constants {

    public static final String TOPIC_IN  = "topic.in";
    public static final String TOPIC_MID = "topic.mid";
    public static final String TOPIC_OUT = "topic.out";

    public static final String KAFKA_BOOTSTRAP_DEFAULT = "localhost:19092";

    public static final long CHECKPOINT_INTERVAL_MS          = 10_000L;
    public static final long CHECKPOINT_TIMEOUT_MS           = 60_000L;
    public static final int  MIN_PAUSE_BETWEEN_CHECKPOINTS_MS = 500;

    public static final String CRASH_AFTER_RECORDS_ENV = "CRASH_AFTER_RECORDS";
    public static final String KAFKA_BOOTSTRAP_ENV     = "KAFKA_BOOTSTRAP";
    public static final String CHECKPOINT_DIR_ENV      = "CHECKPOINT_DIR";

    public static final String PG_URL_ENV      = "PG_URL";
    public static final String PG_USER_ENV     = "PG_USER";
    public static final String PG_PASSWORD_ENV = "PG_PASSWORD";

    public static final String PG_URL_DEFAULT      = "jdbc:postgresql://localhost:15432/workshop";
    public static final String PG_USER_DEFAULT     = "workshop";
    public static final String PG_PASSWORD_DEFAULT = "workshop";
    public static final String PG_TABLE_TRADES     = "processed_trades";

    private Constants() {}
}
